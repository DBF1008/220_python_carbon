import os
import shutil
import tempfile
import unittest

from mock import Mock, patch

from carbon import relayrules, routers
from carbon.exceptions import CarbonConfigException
from carbon.util import parseDestinations
from carbon.tests import util


# Three destinations the router knows about; rules below send traffic to a
# subset of them so routing decisions are observable.
DESTINATIONS = (
    'foo:124:a',
    'foo:125:b',
    'foo:126:c',
)

# Routing destination tuples, e.g. ('foo', 124, 'a').
A, B, C = (parseDestinations([d])[0] for d in DESTINATIONS)


# Rules that send everything to A.
RULES_DEFAULT_ONLY = """
[default]
default = true
destinations = foo:124:a
"""

# A pattern rule (-> B) in front of the default (-> A).
RULES_PATTERN_THEN_DEFAULT = """
[capture]
pattern = ^metric\\.
destinations = foo:125:b

[default]
default = true
destinations = foo:124:a
"""

# Two overlapping pattern rules: the specific one (-> B) precedes the broad
# one (-> C); both match "metric.foo.bar".
RULES_SPECIFIC_THEN_BROAD = """
[specific]
pattern = ^metric\\.foo
destinations = foo:125:b

[broad]
pattern = ^metric\\.
destinations = foo:126:c

[default]
default = true
destinations = foo:124:a
"""

# Same two pattern rules with their order swapped (broad -> C first).
RULES_BROAD_THEN_SPECIFIC = """
[broad]
pattern = ^metric\\.
destinations = foo:126:c

[specific]
pattern = ^metric\\.foo
destinations = foo:125:b

[default]
default = true
destinations = foo:124:a
"""

# Structurally invalid: a section without a 'destinations' list makes
# loadRelayRules raise.
RULES_INVALID = """
[broken]
pattern = ^metric\\.
"""


def write_rules(path, content, mtime):
  """Write a rules file and stamp it with an explicit mtime.

  An explicit, strictly increasing mtime lets the tests drive the manager's
  "only reload when the file changed" gate deterministically, regardless of
  filesystem timestamp resolution.
  """
  with open(path, 'w') as f:
    f.write(content)
  os.utime(path, (mtime, mtime))


def route(router, metric):
  return list(router.getDestinations(metric))


class RelayRulesReloadTest(unittest.TestCase):
  """End-to-end: editing the rules file changes how the router routes."""

  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()
    self.path = os.path.join(self.tmpdir, 'relay-rules.conf')
    self.addCleanup(shutil.rmtree, self.tmpdir)

  def _stop_task(self, task):
    # trial fails on a reactor left with pending calls, so cancel the poll.
    if task.running:
      task.stop()

  def make_router(self):
    settings = util.TestSettings()
    settings['relay-rules'] = self.path
    router = routers.RelayRulesRouter(settings)
    self.addCleanup(self._stop_task, router.rules_manager.read_task)
    for destination in DESTINATIONS:
      router.addDestination(parseDestinations([destination])[0])
    return router

  def reload(self, router):
    # Simulate a poll tick (what the LoopingCall would invoke).
    router.rules_manager.read_rules()

  def test_added_rule_takes_effect(self):
    """A rule added to the file starts matching without a restart."""
    write_rules(self.path, RULES_DEFAULT_ONLY, mtime=1000)
    router = self.make_router()
    self.assertEqual(route(router, 'metric.foo'), [A])

    write_rules(self.path, RULES_PATTERN_THEN_DEFAULT, mtime=2000)
    self.reload(router)

    self.assertEqual(route(router, 'metric.foo'), [B])  # new rule wins
    self.assertEqual(route(router, 'other.thing'), [A])  # default still works

  def test_deleted_rule_takes_effect(self):
    """Removing a rule from the file stops it from matching."""
    write_rules(self.path, RULES_PATTERN_THEN_DEFAULT, mtime=1000)
    router = self.make_router()
    self.assertEqual(route(router, 'metric.foo'), [B])

    write_rules(self.path, RULES_DEFAULT_ONLY, mtime=2000)
    self.reload(router)

    self.assertEqual(route(router, 'metric.foo'), [A])  # falls through to default

  def test_reordered_rules_take_effect(self):
    """Swapping rule order changes which overlapping rule wins."""
    write_rules(self.path, RULES_SPECIFIC_THEN_BROAD, mtime=1000)
    router = self.make_router()
    self.assertEqual(route(router, 'metric.foo.bar'), [B])  # specific first

    write_rules(self.path, RULES_BROAD_THEN_SPECIFIC, mtime=2000)
    self.reload(router)

    self.assertEqual(route(router, 'metric.foo.bar'), [C])  # broad now first

  def test_bad_config_keeps_previous_rules(self):
    """An invalid reload is ignored; the last working rules are preserved."""
    write_rules(self.path, RULES_PATTERN_THEN_DEFAULT, mtime=1000)
    router = self.make_router()
    self.assertEqual(route(router, 'metric.foo'), [B])
    previous_rules = router.rules

    write_rules(self.path, RULES_INVALID, mtime=2000)
    self.reload(router)

    # Routing is unchanged and the exact same rule objects are still in use.
    self.assertEqual(route(router, 'metric.foo'), [B])
    self.assertEqual(route(router, 'other.thing'), [A])
    self.assertIs(router.rules, previous_rules)

  def test_recovers_after_bad_config(self):
    """After a rejected bad config, a later valid config still loads."""
    write_rules(self.path, RULES_PATTERN_THEN_DEFAULT, mtime=1000)
    router = self.make_router()

    write_rules(self.path, RULES_INVALID, mtime=2000)
    self.reload(router)
    self.assertEqual(route(router, 'metric.foo'), [B])  # bad config rejected

    write_rules(self.path, RULES_DEFAULT_ONLY, mtime=3000)
    self.reload(router)
    self.assertEqual(route(router, 'metric.foo'), [A])  # recovered

  def test_missing_file_keeps_previous_rules(self):
    """A file that vanishes between polls leaves routing untouched."""
    write_rules(self.path, RULES_PATTERN_THEN_DEFAULT, mtime=1000)
    router = self.make_router()
    previous_rules = router.rules

    os.remove(self.path)
    self.reload(router)

    self.assertEqual(route(router, 'metric.foo'), [B])
    self.assertIs(router.rules, previous_rules)

  def test_unchanged_mtime_skips_reload(self):
    """If the mtime hasn't advanced the file is not re-read."""
    write_rules(self.path, RULES_PATTERN_THEN_DEFAULT, mtime=1000)
    router = self.make_router()
    previous_rules = router.rules

    # New content but the same mtime: the gate must skip it.
    write_rules(self.path, RULES_DEFAULT_ONLY, mtime=1000)
    self.reload(router)

    self.assertIs(router.rules, previous_rules)
    self.assertEqual(route(router, 'metric.foo'), [B])


class RelayRulesManagerTest(unittest.TestCase):
  """Lower-level checks on the manager's reload plumbing."""

  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()
    self.path = os.path.join(self.tmpdir, 'relay-rules.conf')
    self.addCleanup(shutil.rmtree, self.tmpdir)

  def _stop_task(self, task):
    if task.running:
      task.stop()

  def make_manager(self, content=RULES_DEFAULT_ONLY, mtime=1000):
    write_rules(self.path, content, mtime)
    manager = relayrules.RelayRulesManager()
    manager.read_from(self.path)
    self.addCleanup(self._stop_task, manager.read_task)
    return manager

  def test_looping_call_reads_rules(self):
    manager = relayrules.RelayRulesManager()
    self.assertEqual(manager.read_rules, manager.read_task.f)

  def test_read_from_loads_rules_and_starts_polling(self):
    manager = self.make_manager()
    self.assertEqual(len(manager.rules), 1)
    self.assertTrue(manager.read_task.running)

  def test_read_from_starts_task_once(self):
    write_rules(self.path, RULES_DEFAULT_ONLY, mtime=1000)
    manager = relayrules.RelayRulesManager()
    with patch.object(manager.read_task, 'start') as start_mock:
      manager.read_from(self.path)
      self.assertEqual(1, start_mock.call_count)
    # start was mocked, so the task never actually scheduled anything.

  def test_read_from_is_idempotent(self):
    """Calling read_from again must not blow up on an already-running task."""
    manager = self.make_manager()
    manager.read_from(self.path)  # would raise without the running guard
    self.assertTrue(manager.read_task.running)

  def test_initial_load_is_strict_on_bad_file(self):
    """The first load fails fast, so a misconfigured relay won't start."""
    write_rules(self.path, RULES_INVALID, mtime=1000)
    manager = relayrules.RelayRulesManager()
    self.assertRaises(CarbonConfigException, manager.read_from, self.path)
    self.assertFalse(manager.read_task.running)

  def test_initial_load_is_strict_on_missing_file(self):
    manager = relayrules.RelayRulesManager()
    missing = os.path.join(self.tmpdir, 'does-not-exist.conf')
    self.assertRaises(CarbonConfigException, manager.read_from, missing)

  def test_stat_failure_keeps_previous_rules(self):
    manager = self.make_manager()
    previous_rules = manager.rules

    with patch.object(relayrules, 'getmtime', Mock(side_effect=OSError)):
      manager.read_rules()

    self.assertIs(manager.rules, previous_rules)

  def test_bad_config_does_not_reread_same_version(self):
    """A rejected version advances the marker so it isn't retried each tick."""
    manager = self.make_manager(RULES_DEFAULT_ONLY, mtime=1000)

    write_rules(self.path, RULES_INVALID, mtime=2000)
    manager.read_rules()
    self.assertEqual(manager.rules_last_read, 2000)

    # Same (bad) mtime again: loadRelayRules must not even be called.
    with patch.object(relayrules, 'loadRelayRules') as load_mock:
      manager.read_rules()
      self.assertFalse(load_mock.called)


if __name__ == '__main__':
  unittest.main()
