"""Regression tests for carbon.writer.TagQueue.

These exercise the bounded, de-duplicated, priority-preserving tag queue:
duplicate metrics, the create-supersedes-update rule, TAG_UPDATE_INTERVAL
sampling, queue pressure (bounding), batch boundaries, and thread safety.
"""

import os
import threading
from unittest import TestCase


# carbon.writer loads storage schemas at import time. carbon.storage bakes its
# config-file paths from settings.CONF_DIR on first import, so we point CONF_DIR
# at the existing test fixture conf directory before importing carbon.writer.
# Using the real fixture (rather than an empty/temp dir) keeps those baked paths
# valid for other tests such as test_storage. The bootstrap lives in a function
# so the carbon settings object (whose attribute access raises KeyError for
# missing keys) is not left as a module global, which would otherwise break
# trial's test-class discovery.
def _bootstrap_conf_dir():
  from carbon.conf import settings
  settings['CONF_DIR'] = os.path.join(
      os.path.dirname(os.path.realpath(__file__)), 'data', 'conf-directory')


_bootstrap_conf_dir()

from carbon.writer import TagQueue  # noqa: E402


class TagQueueTest(TestCase):

  def drain(self, q, batch_size=1000):
    """Drain the whole queue into a single list via repeated getbatch calls."""
    result = []
    while True:
      batch = q.getbatch(batch_size)
      if not batch:
        break
      result.extend(batch)
    return result

  # ---- duplicate metric de-duplication ----

  def test_add_dedupes_repeated_metric(self):
    q = TagQueue(update_interval=1)
    for _ in range(5):
      q.add('hot.metric;tag=1')
    self.assertEqual(q.getbatch(10), ['hot.metric;tag=1'])

  def test_update_dedupes_repeated_metric(self):
    q = TagQueue(update_interval=1)
    for _ in range(5):
      q.update('hot.metric;tag=1')
    self.assertEqual(q.getbatch(10), ['hot.metric;tag=1'])

  # ---- create supersedes update / priority ----

  def test_update_then_add_yields_single_create(self):
    q = TagQueue(update_interval=1)
    q.update('m;tag=1')
    q.add('m;tag=1')
    self.assertEqual(q.getbatch(10), ['m;tag=1'])
    # The pending update was absorbed by the create; nothing lingers.
    self.assertEqual(q.getbatch(10), [])

  def test_add_then_update_skips_redundant_update(self):
    q = TagQueue(update_interval=1)
    q.add('m;tag=1')
    q.update('m;tag=1')
    self.assertEqual(q.getbatch(10), ['m;tag=1'])
    self.assertEqual(q.getbatch(10), [])

  def test_creates_drained_before_updates(self):
    q = TagQueue(update_interval=1)
    q.add('a;tag=1')
    q.update('b;tag=1')
    self.assertEqual(q.getbatch(10), ['a;tag=1', 'b;tag=1'])

  def test_update_resumes_after_create_drained(self):
    # Once a create has been sent, later updates for that metric enqueue again.
    q = TagQueue(update_interval=1)
    q.add('m;tag=1')
    self.assertEqual(q.getbatch(10), ['m;tag=1'])
    q.update('m;tag=1')
    self.assertEqual(q.getbatch(10), ['m;tag=1'])

  # ---- TAG_UPDATE_INTERVAL (global 1-in-N sampling) ----

  def test_update_interval_one_enqueues_every_metric(self):
    q = TagQueue(update_interval=1)
    for i in range(5):
      q.update('m%d;tag=1' % i)
    self.assertEqual(q.getbatch(10), ['m%d;tag=1' % i for i in range(5)])

  def test_update_interval_samples_one_in_n_globally(self):
    q = TagQueue(update_interval=3)
    for i in range(9):
      q.update('m%d;tag=1' % i)
    # The counter ticks on every call; only the 1st, 4th and 7th trigger an
    # enqueue, so the cadence of tag-index updates is preserved.
    self.assertEqual(q.getbatch(10), ['m0;tag=1', 'm3;tag=1', 'm6;tag=1'])

  # ---- queue pressure / bounding ----

  def test_add_queue_is_bounded(self):
    q = TagQueue(maxsize=3, update_interval=1)
    for i in range(10):
      q.add('m%d;tag=1' % i)
    # Only the first three fit; the rest are dropped (FIFO, no exception).
    self.assertEqual(q.getbatch(100), ['m0;tag=1', 'm1;tag=1', 'm2;tag=1'])

  def test_update_queue_is_bounded(self):
    q = TagQueue(maxsize=3, update_interval=1)
    for i in range(10):
      q.update('m%d;tag=1' % i)
    self.assertEqual(q.getbatch(100), ['m0;tag=1', 'm1;tag=1', 'm2;tag=1'])

  def test_maxsize_zero_is_unbounded(self):
    q = TagQueue(maxsize=0, update_interval=1)
    for i in range(1000):
      q.add('m%d;tag=1' % i)
    self.assertEqual(len(self.drain(q)), 1000)

  def test_create_supersede_frees_an_update_slot(self):
    # An update removed by a superseding create must not count against the bound.
    q = TagQueue(maxsize=1, update_interval=1)
    q.update('m;tag=1')        # update queue now full (size 1)
    q.add('m;tag=1')           # supersedes -> update queue empty again
    q.update('other;tag=1')    # so this update still fits
    self.assertEqual(sorted(self.drain(q)), ['m;tag=1', 'other;tag=1'])

  # ---- batch boundaries ----

  def test_getbatch_respects_max_batch_size(self):
    q = TagQueue(update_interval=1)
    for i in range(10):
      q.add('m%d;tag=1' % i)
    self.assertEqual(q.getbatch(4), ['m0;tag=1', 'm1;tag=1', 'm2;tag=1', 'm3;tag=1'])
    self.assertEqual(q.getbatch(4), ['m4;tag=1', 'm5;tag=1', 'm6;tag=1', 'm7;tag=1'])
    self.assertEqual(q.getbatch(4), ['m8;tag=1', 'm9;tag=1'])

  def test_getbatch_fills_from_updates_after_adds(self):
    q = TagQueue(update_interval=1)
    q.add('a;tag=1')
    q.add('b;tag=1')
    q.update('c;tag=1')
    q.update('d;tag=1')
    # A single batch spans the add/update boundary, creates first.
    self.assertEqual(q.getbatch(3), ['a;tag=1', 'b;tag=1', 'c;tag=1'])
    self.assertEqual(q.getbatch(3), ['d;tag=1'])

  def test_getbatch_on_empty_queue_returns_empty_list(self):
    q = TagQueue(update_interval=1)
    self.assertEqual(q.getbatch(5), [])

  # ---- thread safety ----

  def test_concurrent_producers_do_not_duplicate(self):
    # Many threads hammering the same metrics must still enqueue each exactly
    # once -- this is what the lock guarantees.
    q = TagQueue(update_interval=1)
    metrics = ['m%d;tag=1' % i for i in range(50)]
    threads = []
    barrier = threading.Barrier(8, timeout=30)

    def producer():
      barrier.wait()
      for _ in range(20):
        for m in metrics:
          q.add(m)
          q.update(m)

    for _ in range(8):
      t = threading.Thread(target=producer)
      threads.append(t)
      t.start()
    for t in threads:
      t.join(timeout=30)
      self.assertFalse(t.is_alive())

    drained = self.drain(q)
    self.assertEqual(len(drained), len(set(drained)))   # no duplicates
    self.assertEqual(sorted(drained), sorted(metrics))   # exactly the distinct set
