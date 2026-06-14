import threading
from unittest import TestCase

from carbon.writer import TagQueue


class TagQueueBasicTest(TestCase):
  """Basic add / update / getbatch operations."""

  def test_empty_queue_getbatch(self):
    q = TagQueue()
    self.assertEqual([], q.getbatch(10))

  def test_empty_queue_len(self):
    q = TagQueue()
    self.assertEqual(0, len(q))

  def test_add_single(self):
    q = TagQueue()
    q.add('m1')
    self.assertEqual(['m1'], q.getbatch(10))

  def test_update_single_interval_1(self):
    q = TagQueue(update_interval=1)
    q.update('m1')
    self.assertEqual(['m1'], q.getbatch(10))

  def test_add_multiple_preserves_insertion_order(self):
    q = TagQueue()
    for m in ['a', 'b', 'c']:
      q.add(m)
    self.assertEqual(['a', 'b', 'c'], q.getbatch(10))

  def test_update_multiple_preserves_insertion_order(self):
    q = TagQueue(update_interval=1)
    for m in ['a', 'b', 'c']:
      q.update(m)
    self.assertEqual(['a', 'b', 'c'], q.getbatch(10))

  def test_len_reflects_pending(self):
    q = TagQueue()
    q.add('a')
    q.add('b')
    self.assertEqual(2, len(q))
    q.getbatch(1)
    self.assertEqual(1, len(q))
    q.getbatch(10)
    self.assertEqual(0, len(q))


class TagQueueDeduplicationTest(TestCase):
  """Same metric must appear at most once regardless of call frequency."""

  def test_duplicate_adds(self):
    q = TagQueue()
    for _ in range(100):
      q.add('hot.metric')
    self.assertEqual(['hot.metric'], q.getbatch(100))

  def test_duplicate_updates(self):
    q = TagQueue(update_interval=1)
    for _ in range(100):
      q.update('hot.metric')
    self.assertEqual(['hot.metric'], q.getbatch(100))

  def test_duplicate_updates_with_interval(self):
    """Even with interval=10, the same metric is enqueued at most once."""
    q = TagQueue(update_interval=10)
    for _ in range(50):
      q.update('hot.metric')
    # interval=10 → fires at calls 10, 20, 30, 40, 50 → 5 times,
    # but dedup means it's still only in the set once.
    self.assertEqual(['hot.metric'], q.getbatch(100))

  def test_mixed_add_and_update_same_metric(self):
    """add() then update() for the same metric: only the add is present."""
    q = TagQueue(update_interval=1)
    q.add('m1')
    q.update('m1')
    q.update('m1')
    self.assertEqual(['m1'], q.getbatch(100))

  def test_dedup_counter_increments(self):
    q = TagQueue()
    q.add('m1')
    q.add('m1')
    q.add('m1')
    self.assertEqual(2, q.deduped)

  def test_many_unique_metrics_no_dedup(self):
    q = TagQueue()
    for i in range(50):
      q.add('metric.%d' % i)
    batch = q.getbatch(100)
    self.assertEqual(50, len(batch))
    self.assertEqual(0, q.deduped)


class TagQueuePriorityTest(TestCase):
  """First-time creates must be drained before periodic updates."""

  def test_adds_before_updates(self):
    q = TagQueue(update_interval=1)
    q.update('u1')
    q.update('u2')
    q.add('a1')
    q.add('a2')
    batch = q.getbatch(10)
    self.assertEqual(['a1', 'a2', 'u1', 'u2'], batch)

  def test_interleaved_adds_and_updates_ordering(self):
    q = TagQueue(update_interval=1)
    q.add('a1')
    q.update('u1')
    q.add('a2')
    q.update('u2')
    batch = q.getbatch(10)
    self.assertEqual(['a1', 'a2', 'u1', 'u2'], batch)

  def test_getbatch_drains_adds_first_respecting_maxsize(self):
    q = TagQueue(update_interval=1)
    q.add('a1')
    q.add('a2')
    q.update('u1')
    q.update('u2')
    # maxsize=2 should only return adds
    batch = q.getbatch(2)
    self.assertEqual(['a1', 'a2'], batch)
    # next batch gets updates
    batch = q.getbatch(2)
    self.assertEqual(['u1', 'u2'], batch)


class TagQueuePromotionTest(TestCase):
  """A metric in the update lane is promoted to the add lane on add()."""

  def test_promote_update_to_add(self):
    q = TagQueue(update_interval=1)
    q.update('m1')
    q.add('m1')
    # Should appear exactly once and in the add position.
    batch = q.getbatch(10)
    self.assertEqual(['m1'], batch)

  def test_promote_preserves_other_ordering(self):
    q = TagQueue(update_interval=1)
    q.update('u1')
    q.update('m1')
    q.update('u2')
    # Promote m1 to add lane
    q.add('m1')
    batch = q.getbatch(10)
    # m1 is now an add (drained first), then remaining updates in order
    self.assertEqual(['m1', 'u1', 'u2'], batch)

  def test_add_then_update_stays_in_add_lane(self):
    q = TagQueue(update_interval=1)
    q.add('m1')
    q.update('m1')
    # m1 stays in add lane only
    self.assertEqual(1, len(q))
    batch = q.getbatch(10)
    self.assertEqual(['m1'], batch)


class TagQueueThrottlingTest(TestCase):
  """Per-metric TAG_UPDATE_INTERVAL throttling."""

  def test_interval_1_fires_every_call(self):
    q = TagQueue(update_interval=1)
    q.update('m1')
    self.assertEqual(1, len(q))

  def test_interval_3_fires_at_third_call(self):
    q = TagQueue(update_interval=3)
    q.update('m1')
    q.update('m1')
    self.assertEqual(0, len(q))
    q.update('m1')
    self.assertEqual(1, len(q))

  def test_interval_resets_after_firing(self):
    q = TagQueue(update_interval=3)
    # First firing at call 3
    for _ in range(3):
      q.update('m1')
    self.assertEqual(1, len(q))
    q.getbatch(10)  # drain
    # Counter was reset. Next firing at call 3 again.
    q.update('m1')
    q.update('m1')
    self.assertEqual(0, len(q))
    q.update('m1')
    self.assertEqual(1, len(q))

  def test_per_metric_independent_counters(self):
    """Each metric has its own independent counter."""
    q = TagQueue(update_interval=3)
    q.update('m1')
    q.update('m1')
    q.update('m2')
    q.update('m2')
    # Neither has reached interval=3 yet
    self.assertEqual(0, len(q))
    q.update('m1')  # m1 fires (3rd call)
    q.update('m2')  # m2 fires (3rd call)
    self.assertEqual(2, len(q))
    batch = q.getbatch(10)
    self.assertEqual(sorted(batch), ['m1', 'm2'])

  def test_interval_0_treated_as_1(self):
    """update_interval=0 is clamped to 1 (fire every call)."""
    q = TagQueue(update_interval=0)
    q.update('m1')
    self.assertEqual(1, len(q))

  def test_counter_cleaned_up_after_getbatch(self):
    """After draining an update, its counter should be cleaned up."""
    q = TagQueue(update_interval=2)
    q.update('m1')
    q.update('m1')  # fires
    self.assertEqual(1, len(q))
    q.getbatch(10)  # drain
    # Counter should be gone; next update starts from 1
    q.update('m1')
    self.assertEqual(0, len(q))  # count=1 < interval=2
    q.update('m1')
    self.assertEqual(1, len(q))  # count=2 >= interval=2


class TagQueueBoundedTest(TestCase):
  """Queue pressure: maxsize enforcement and eviction behavior."""

  def test_maxsize_limits_total_size(self):
    q = TagQueue(maxsize=3)
    q.add('a')
    q.add('b')
    q.add('c')
    self.assertEqual(3, len(q))
    q.add('d')  # should be dropped, no updates to evict
    self.assertEqual(3, len(q))
    self.assertEqual(1, q.dropped)

  def test_add_evicts_oldest_update(self):
    """When full, adding a new metric evicts the oldest update entry."""
    q = TagQueue(maxsize=3, update_interval=1)
    q.update('u1')
    q.update('u2')
    q.update('u3')
    self.assertEqual(3, len(q))
    # Adding a new metric should evict u1 (oldest update)
    q.add('a1')
    self.assertEqual(3, len(q))
    batch = q.getbatch(10)
    # a1 (add) first, then remaining updates
    self.assertEqual(['a1', 'u2', 'u3'], batch)

  def test_update_dropped_when_full(self):
    q = TagQueue(maxsize=2, update_interval=1)
    q.update('m1')
    q.update('m2')
    q.update('m3')  # should be dropped
    self.assertEqual(2, len(q))
    self.assertEqual(1, q.dropped)
    batch = q.getbatch(10)
    self.assertEqual(['m1', 'm2'], batch)

  def test_dropped_counter_accumulates(self):
    q = TagQueue(maxsize=1, update_interval=1)
    q.update('m1')
    q.update('m2')
    q.update('m3')
    self.assertEqual(2, q.dropped)

  def test_unbounded_when_maxsize_zero(self):
    """maxsize=0 means unbounded (matching queue.Queue semantics)."""
    q = TagQueue(maxsize=0)
    for i in range(1000):
      q.add('metric.%d' % i)
    self.assertEqual(1000, len(q))

  def test_add_evicts_multiple_updates_to_make_room(self):
    """Multiple adds should each evict an update when the queue is full."""
    q = TagQueue(maxsize=3, update_interval=1)
    q.update('u1')
    q.update('u2')
    q.update('u3')
    q.add('a1')  # evicts u1
    q.add('a2')  # evicts u2
    self.assertEqual(3, len(q))
    batch = q.getbatch(10)
    self.assertEqual(['a1', 'a2', 'u3'], batch)

  def test_add_dropped_when_only_adds_in_full_queue(self):
    """If queue is full of adds only, new adds are dropped."""
    q = TagQueue(maxsize=2)
    q.add('a1')
    q.add('a2')
    q.add('a3')  # dropped, nothing to evict
    self.assertEqual(2, len(q))
    self.assertEqual(1, q.dropped)
    batch = q.getbatch(10)
    self.assertEqual(['a1', 'a2'], batch)


class TagQueueBatchBoundaryTest(TestCase):
  """Batch boundary: various maxsize edge cases in getbatch()."""

  def test_batch_size_1(self):
    q = TagQueue()
    q.add('a')
    q.add('b')
    self.assertEqual(['a'], q.getbatch(1))
    self.assertEqual(['b'], q.getbatch(1))

  def test_batch_size_exact(self):
    """maxsize equals queue size: returns everything."""
    q = TagQueue()
    q.add('a')
    q.add('b')
    q.add('c')
    batch = q.getbatch(3)
    self.assertEqual(['a', 'b', 'c'], batch)
    self.assertEqual(0, len(q))

  def test_batch_size_larger_than_queue(self):
    """maxsize larger than queue size: returns all available."""
    q = TagQueue()
    q.add('a')
    self.assertEqual(['a'], q.getbatch(100))

  def test_batch_size_zero(self):
    q = TagQueue()
    q.add('a')
    self.assertEqual([], q.getbatch(0))
    self.assertEqual(1, len(q))  # nothing consumed

  def test_batch_spans_adds_and_updates(self):
    """A single batch can contain both adds and updates."""
    q = TagQueue(update_interval=1)
    q.add('a1')
    q.update('u1')
    batch = q.getbatch(2)
    self.assertEqual(['a1', 'u1'], batch)

  def test_repeated_getbatch_drains_completely(self):
    q = TagQueue(update_interval=1)
    for m in ['a', 'b', 'c']:
      q.add(m)
    for m in ['d', 'e']:
      q.update(m)
    all_items = []
    while True:
      batch = q.getbatch(2)
      if not batch:
        break
      all_items.extend(batch)
    self.assertEqual(['a', 'b', 'c', 'd', 'e'], all_items)

  def test_getbatch_returns_unique_items_only(self):
    """Even under adversarial input, batch contains no duplicates."""
    q = TagQueue(update_interval=1)
    for _ in range(100):
      q.add('m1')
      q.update('m1')
      q.add('m2')
      q.update('m2')
    batch = q.getbatch(100)
    self.assertEqual(sorted(batch), ['m1', 'm2'])


class TagQueueConcurrencyTest(TestCase):
  """Thread safety: concurrent add/update/getbatch must not corrupt state."""

  def test_concurrent_producers_and_consumer(self):
    q = TagQueue(maxsize=500, update_interval=1)
    errors = []
    num_producers = 4
    num_items_per_producer = 200
    consumer_batches = []

    def producer(tid):
      try:
        for i in range(num_items_per_producer):
          metric = 'producer.%d.metric.%d' % (tid, i)
          q.add(metric)
          for _ in range(5):
            q.update(metric)
      except Exception as e:
        errors.append(e)

    def consumer():
      try:
        empty_rounds = 0
        while empty_rounds < 20:
          batch = q.getbatch(50)
          if batch:
            consumer_batches.append(batch)
            empty_rounds = 0
          else:
            empty_rounds += 1
      except Exception as e:
        errors.append(e)

    threads = []
    for tid in range(num_producers):
      t = threading.Thread(target=producer, args=(tid,))
      threads.append(t)
    t_consumer = threading.Thread(target=consumer)
    threads.append(t_consumer)

    for t in threads:
      t.start()
    for t in threads:
      t.join(timeout=10)

    self.assertEqual([], errors, "Thread errors occurred: %s" % errors)

    # Drain anything remaining
    while True:
      batch = q.getbatch(100)
      if not batch:
        break
      consumer_batches.append(batch)

    # All consumed items must be unique (no duplicates across batches)
    all_consumed = [m for batch in consumer_batches for m in batch]
    self.assertEqual(len(all_consumed), len(set(all_consumed)),
                     "Duplicate metrics found in consumed batches")

    # Queue should now be empty
    self.assertEqual(0, len(q))


class TagQueueIntegrationScenarioTest(TestCase):
  """End-to-end scenarios mimicking real write patterns."""

  def test_hot_metric_does_not_flood_queue(self):
    """A single metric written 10000 times should occupy 1 slot at most."""
    q = TagQueue(maxsize=1000, update_interval=100)
    # Simulate: metric is created, then written 10000 times
    q.add('hot.metric;tag=value')
    for _ in range(10000):
      q.update('hot.metric;tag=value')
    # Only 1 entry (the add) — updates are deduped/throttled
    self.assertLessEqual(len(q), 2)
    batch = q.getbatch(100)
    self.assertEqual(batch[0], 'hot.metric;tag=value')

  def test_mixed_new_and_existing_metrics(self):
    """Simulate: 5 new metrics created, 20 existing metrics each written many times."""
    q = TagQueue(maxsize=100, update_interval=5)
    # 5 new metrics
    for i in range(5):
      q.add('new.metric.%d' % i)
    # 20 existing metrics, each written 50 times
    for i in range(20):
      for _ in range(50):
        q.update('existing.metric.%d' % i)
    batch = q.getbatch(100)
    # First 5 should be the new metrics (adds have priority)
    adds = batch[:5]
    self.assertEqual(['new.metric.%d' % i for i in range(5)], adds)
    # Remaining are unique updates (at most 20)
    updates = batch[5:]
    self.assertEqual(len(updates), len(set(updates)))
    self.assertLessEqual(len(updates), 20)

  def test_full_cycle_add_drain_update_drain(self):
    """Full lifecycle: add → drain → write → update → drain."""
    q = TagQueue(update_interval=2)
    q.add('m1')
    q.add('m2')
    # Drain adds
    batch = q.getbatch(10)
    self.assertEqual(['m1', 'm2'], batch)
    # Now simulate writes hitting the interval
    q.update('m1')  # count=1
    q.update('m2')  # count=1
    self.assertEqual(0, len(q))
    q.update('m1')  # count=2 >= interval=2, fires
    q.update('m2')  # count=2 >= interval=2, fires
    batch = q.getbatch(10)
    self.assertEqual(sorted(batch), ['m1', 'm2'])
    # Queue is empty again
    self.assertEqual(0, len(q))
