import os
import shutil
import tempfile
import time
import unittest

from carbon import routers
from carbon.exceptions import CarbonConfigException
from carbon.util import parseDestinations
from carbon.tests import util


DESTINATIONS = (
    'foo:124:a',
    'foo:125:b',
    'foo:126:c',
    'bar:423:a',
    'bar:424:b',
    'bar:425:c',
)

# Minimal valid rules file content used across reload tests.
DEFAULT_RULES = (
    "[default]\n"
    "destinations = foo:124:a\n"
    "default = true\n"
)


def createSettings():
    settings = util.TestSettings()
    settings['DIVERSE_REPLICAS'] = True,
    settings['REPLICATION_FACTOR'] = 2
    settings['DESTINATIONS'] = DESTINATIONS
    settings['relay-rules'] = os.path.join(
        os.path.dirname(__file__), 'relay-rules.conf')
    settings['aggregation-rules'] = None
    return settings


def parseDestination(destination):
    return parseDestinations([destination])[0]


class TestRelayRulesRouter(unittest.TestCase):
    def testBasic(self):
        router = routers.RelayRulesRouter(createSettings())
        self.addCleanup(router.stop)
        for destination in DESTINATIONS:
            router.addDestination(parseDestination(destination))
        self.assertEqual(len(list(router.getDestinations('foo.bar'))), 1)


class TestOtherRouters(unittest.TestCase):
    def testBasic(self):
        settings = createSettings()
        for plugin in routers.DatapointRouter.plugins:
            # Test everything except 'rules' which is special
            if plugin == 'rules':
                continue

            router = routers.DatapointRouter.plugins[plugin](settings)
            self.assertEqual(len(list(router.getDestinations('foo.bar'))), 0)

            for destination in DESTINATIONS:
                router.addDestination(parseDestination(destination))
            self.assertEqual(
                len(list(router.getDestinations('foo.bar'))),
                len(DESTINATIONS) if plugin == 'constant' else settings['REPLICATION_FACTOR']
            )


class RelayRulesRouterReloadTest(unittest.TestCase):
    """Regression tests for relay rules hot-reload."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.rules_file = os.path.join(self.tmpdir, 'relay-rules.conf')

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    # -- helpers ---------------------------------------------------------------

    def _make_router(self, content=None):
        """Create a router with the given rules content; stop LoopingCall."""
        if content is not None:
            with open(self.rules_file, 'w') as f:
                f.write(content)
        settings = util.TestSettings()
        settings['relay-rules'] = self.rules_file
        router = routers.RelayRulesRouter(settings)
        router.stop()
        return router

    def _write_rules(self, content):
        with open(self.rules_file, 'w') as f:
            f.write(content)

    def _touch(self):
        """Bump mtime to guarantee it advances past the last-read timestamp."""
        old_mtime = os.path.getmtime(self.rules_file)
        new_mtime = old_mtime + 1
        os.utime(self.rules_file, (new_mtime, new_mtime))

    # -- initial load ----------------------------------------------------------

    def test_initial_load(self):
        router = self._make_router(DEFAULT_RULES)
        self.assertEqual(len(router.rules), 1)
        # The only rule is the default (catch-all) rule.
        self.assertTrue(router.rules[0].matches('anything'))

    def test_initial_load_missing_file_raises(self):
        settings = util.TestSettings()
        settings['relay-rules'] = '/nonexistent/path/relay-rules.conf'
        with self.assertRaises(CarbonConfigException):
            routers.RelayRulesRouter(settings)

    def test_initial_load_bad_config_raises(self):
        self._write_rules("[broken]\npattern = ^x\\.\n")
        settings = util.TestSettings()
        settings['relay-rules'] = self.rules_file
        with self.assertRaises(CarbonConfigException):
            routers.RelayRulesRouter(settings)

    # -- adding rules ----------------------------------------------------------

    def test_add_new_rule_on_reload(self):
        router = self._make_router(DEFAULT_RULES)
        self.assertEqual(len(router.rules), 1)

        self._write_rules(
            "[prod]\n"
            "pattern = ^prod\\.\n"
            "destinations = bar:124:b\n"
            "\n"
            "[default]\n"
            "destinations = foo:124:a\n"
            "default = true\n"
        )
        self._touch()
        router.read_rules()

        self.assertEqual(len(router.rules), 2)
        # First rule is the pattern rule; second is the default.
        self.assertTrue(router.rules[0].matches('prod.cpu'))
        self.assertFalse(router.rules[0].matches('dev.cpu'))

    # -- deleting rules --------------------------------------------------------

    def test_delete_rule_on_reload(self):
        router = self._make_router(
            "[prod]\n"
            "pattern = ^prod\\.\n"
            "destinations = bar:124:b\n"
            "\n"
            "[default]\n"
            "destinations = foo:124:a\n"
            "default = true\n"
        )
        self.assertEqual(len(router.rules), 2)

        self._write_rules(DEFAULT_RULES)
        self._touch()
        router.read_rules()

        self.assertEqual(len(router.rules), 1)

    # -- reordering rules ------------------------------------------------------

    def test_change_rule_order_on_reload(self):
        router = self._make_router(
            "[alpha]\n"
            "pattern = ^a\\.\n"
            "destinations = a:124:a\n"
            "\n"
            "[beta]\n"
            "pattern = ^b\\.\n"
            "destinations = b:124:b\n"
            "\n"
            "[default]\n"
            "destinations = d:124:d\n"
            "default = true\n"
        )
        self.assertEqual(router.rules[0].destinations, [('a', 124, 'a')])
        self.assertEqual(router.rules[1].destinations, [('b', 124, 'b')])

        # Swap alpha and beta
        self._write_rules(
            "[beta]\n"
            "pattern = ^b\\.\n"
            "destinations = b:124:b\n"
            "\n"
            "[alpha]\n"
            "pattern = ^a\\.\n"
            "destinations = a:124:a\n"
            "\n"
            "[default]\n"
            "destinations = d:124:d\n"
            "default = true\n"
        )
        self._touch()
        router.read_rules()

        self.assertEqual(router.rules[0].destinations, [('b', 124, 'b')])
        self.assertEqual(router.rules[1].destinations, [('a', 124, 'a')])

    # -- bad config fallback ---------------------------------------------------

    def test_bad_config_keeps_old_rules(self):
        router = self._make_router(DEFAULT_RULES)
        old_rules = router.rules
        self.assertEqual(len(old_rules), 1)

        # Write invalid config: section without 'destinations'.
        self._write_rules(
            "[broken]\n"
            "pattern = ^x\\.\n"
        )
        self._touch()
        router.read_rules()

        # Rules must be the same object — nothing was swapped.
        self.assertIs(router.rules, old_rules)

    def test_missing_default_keeps_old_rules(self):
        router = self._make_router(DEFAULT_RULES)
        old_rules = router.rules

        # Write config without a default section.
        self._write_rules(
            "[prod]\n"
            "pattern = ^prod\\.\n"
            "destinations = bar:124:b\n"
        )
        self._touch()
        router.read_rules()

        self.assertIs(router.rules, old_rules)

    def test_bad_regex_keeps_old_rules(self):
        router = self._make_router(DEFAULT_RULES)
        old_rules = router.rules

        # Write config with an invalid regex pattern.
        self._write_rules(
            "[broken]\n"
            "pattern = [invalid\n"
            "destinations = bar:124:b\n"
            "\n"
            "[default]\n"
            "destinations = foo:124:a\n"
            "default = true\n"
        )
        self._touch()
        router.read_rules()

        self.assertIs(router.rules, old_rules)

    # -- file removed at runtime -----------------------------------------------

    def test_file_removed_clears_rules(self):
        router = self._make_router(DEFAULT_RULES)
        self.assertEqual(len(router.rules), 1)

        os.remove(self.rules_file)
        router.read_rules()

        # Missing file at runtime clears rules (matches _RewriteRuleManager).
        self.assertEqual(router.rules, [])

    # -- mtime unchanged -------------------------------------------------------

    def test_mtime_unchanged_skips_reload(self):
        router = self._make_router(DEFAULT_RULES)
        original_rules = router.rules

        # Call read_rules again without modifying the file.
        router.read_rules()

        # Must be the exact same object — no re-parse occurred.
        self.assertIs(router.rules, original_rules)

    # -- destinations independence ---------------------------------------------

    def test_destinations_survive_reload(self):
        router = self._make_router(DEFAULT_RULES)
        router.addDestination(('foo', 124, 'a'))

        self._write_rules(
            "[prod]\n"
            "pattern = ^prod\\.\n"
            "destinations = bar:124:b\n"
            "\n"
            "[default]\n"
            "destinations = foo:124:a\n"
            "default = true\n"
        )
        self._touch()
        router.read_rules()

        # The destinations set is independent of rule reload.
        self.assertIn(('foo', 124, 'a'), router.destinations)

    def test_reload_routes_reroutes_metrics(self):
        """End-to-end: after reload, getDestinations reflects the new rules."""
        router = self._make_router(
            "[prod]\n"
            "pattern = ^prod\\.\n"
            "destinations = a:124:a\n"
            "\n"
            "[default]\n"
            "destinations = d:124:d\n"
            "default = true\n"
        )
        router.addDestination(('a', 124, 'a'))
        router.addDestination(('d', 124, 'd'))

        # Before reload: prod.cpu → a:124:a
        dests = list(router.getDestinations('prod.cpu'))
        self.assertEqual(dests, [('a', 124, 'a')])

        # Swap the prod rule to point at d:124:d instead.
        self._write_rules(
            "[prod]\n"
            "pattern = ^prod\\.\n"
            "destinations = d:124:d\n"
            "\n"
            "[default]\n"
            "destinations = a:124:a\n"
            "default = true\n"
        )
        self._touch()
        router.read_rules()

        # After reload: prod.cpu → d:124:d
        dests = list(router.getDestinations('prod.cpu'))
        self.assertEqual(dests, [('d', 124, 'd')])

    # -- stop is idempotent ----------------------------------------------------

    def test_stop_idempotent(self):
        router = self._make_router(DEFAULT_RULES)
        router.stop()  # already stopped by _make_router; must not raise
        router.stop()
