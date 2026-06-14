"""Copyright 2009 Chris Davis

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import threading
import time
from collections import OrderedDict

from carbon import state
from carbon.cache import MetricCache
from carbon.storage import loadStorageSchemas, loadAggregationSchemas
from carbon.conf import settings
from carbon import log, instrumentation
from carbon.util import TokenBucket

from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from twisted.application.service import Service

try:
    import signal
except ImportError:
    log.msg("Couldn't import signal module")


SCHEMAS = loadStorageSchemas()
AGGREGATION_SCHEMAS = loadAggregationSchemas()


# Initialize token buckets so that we can enforce rate limits on creates and
# updates if the config wants them.
CREATE_BUCKET = None
UPDATE_BUCKET = None
if settings.MAX_CREATES_PER_MINUTE != float('inf'):
  capacity = settings.MAX_CREATES_PER_MINUTE
  fill_rate = float(settings.MAX_CREATES_PER_MINUTE) / 60
  CREATE_BUCKET = TokenBucket(capacity, fill_rate)

if settings.MAX_UPDATES_PER_SECOND != float('inf'):
  capacity = settings.MAX_UPDATES_PER_SECOND
  fill_rate = settings.MAX_UPDATES_PER_SECOND
  UPDATE_BUCKET = TokenBucket(capacity, fill_rate)


class TagQueue(object):
  """Bounded, deduplicated, priority-aware batch queue for tag updates.

  Maintains two logical lanes:
  - *add*: first-time metric creation tags (highest priority, drained first).
  - *update*: periodic re-tags throttled by per-metric TAG_UPDATE_INTERVAL.

  A given metric appears in at most one lane at a time. Adding a metric
  that is already in the update lane promotes it to the add lane. Updates
  for a metric already in the add lane are silently dropped (the pending
  add will already inform the tag database of the series' existence).
  """

  def __init__(self, maxsize=0, update_interval=1):
    self.maxsize = maxsize
    self.update_interval = max(1, update_interval)
    self._lock = threading.Lock()
    # Add lane: metrics needing their first tag call (highest priority).
    self._add_set = OrderedDict()
    # Update lane: metrics needing periodic re-tagging.
    self._update_set = OrderedDict()
    # Per-metric write counter for TAG_UPDATE_INTERVAL throttling.
    # Tracks how many update() calls have occurred since the last time
    # this metric was enqueued for an update.
    self._update_counters = {}
    # Instrumentation counters (not thread-safe reads, but good enough
    # for periodic sampling).
    self.dropped = 0
    self.deduped = 0

  def _total_size(self):
    return len(self._add_set) + len(self._update_set)

  def add(self, metric):
    """Enqueue a first-time creation tag for *metric*.

    If the metric is already in the add lane this is a no-op (dedup).
    If it is in the update lane it is promoted to the add lane.
    """
    with self._lock:
      if metric in self._add_set:
        self.deduped += 1
        return
      # Promote from update lane if present.
      if metric in self._update_set:
        del self._update_set[metric]
        self._update_counters.pop(metric, None)
      # Enforce bound: evict the oldest update entry if needed.
      if self.maxsize and self._total_size() >= self.maxsize:
        if self._update_set:
          evicted, _ = self._update_set.popitem(last=False)
          self._update_counters.pop(evicted, None)
          self.dropped += 1
        else:
          self.dropped += 1
          return
      self._add_set[metric] = True
      # Fresh add: reset any stale counter.
      self._update_counters.pop(metric, None)

  def update(self, metric):
    """Schedule a periodic re-tag for *metric*, subject to throttling.

    Only one update per metric is enqueued for every *update_interval*
    calls. If an add is already pending for the same metric the call is
    dropped (the add will cover it).
    """
    with self._lock:
      if metric in self._add_set:
        self.deduped += 1
        return
      # Per-metric throttle.
      count = self._update_counters.get(metric, 0) + 1
      if count < self.update_interval:
        self._update_counters[metric] = count
        return
      self._update_counters[metric] = 0
      if metric in self._update_set:
        self.deduped += 1
        return
      if self.maxsize and self._total_size() >= self.maxsize:
        self.dropped += 1
        return
      self._update_set[metric] = True

  def getbatch(self, maxsize=1):
    """Return up to *maxsize* unique metrics, adds before updates."""
    batch = []
    with self._lock:
      while len(batch) < maxsize and self._add_set:
        metric, _ = self._add_set.popitem(last=False)
        batch.append(metric)
      while len(batch) < maxsize and self._update_set:
        metric, _ = self._update_set.popitem(last=False)
        self._update_counters.pop(metric, None)
        batch.append(metric)
    return batch

  def __len__(self):
    with self._lock:
      return self._total_size()


tagQueue = TagQueue(maxsize=settings.TAG_QUEUE_SIZE, update_interval=settings.TAG_UPDATE_INTERVAL)


def writeCachedDataPoints():
  "Write datapoints until the MetricCache is completely empty"

  cache = MetricCache()
  while cache:
    # First, create new metrics files, which is helpful for graphite-web
    while cache.new_metrics and (not CREATE_BUCKET or CREATE_BUCKET.peek(1)):
      metric = cache.new_metrics.popleft()

      if metric not in cache:
        # This metric has already been drained. There's no sense in creating it.
        continue

      if state.database.exists(metric):
        continue

      if CREATE_BUCKET and not CREATE_BUCKET.drain(1):
        # This should never actually happen as no other thread should be
        # draining our tokens, and we just checked for a token.
        # Just put the new metric back in the create list and we'll try again
        # after writing an update.
        cache.new_metrics.appendleft(metric)
        break

      archiveConfig = None
      xFilesFactor, aggregationMethod = None, None

      for schema in SCHEMAS:
        if schema.matches(metric):
          if settings.LOG_CREATES:
            log.creates('new metric %s matched schema %s' % (metric, schema.name))
          archiveConfig = [archive.getTuple() for archive in schema.archives]
          break

      for schema in AGGREGATION_SCHEMAS:
        if schema.matches(metric):
          if settings.LOG_CREATES:
            log.creates('new metric %s matched aggregation schema %s'
                        % (metric, schema.name))
          xFilesFactor, aggregationMethod = schema.archives
          break

      if not archiveConfig:
        raise Exception(("No storage schema matched the metric '%s',"
                         " check your storage-schemas.conf file.") % metric)

      if settings.LOG_CREATES:
        log.creates("creating database metric %s (archive=%s xff=%s agg=%s)" %
                    (metric, archiveConfig, xFilesFactor, aggregationMethod))
      try:
        state.database.create(metric, archiveConfig, xFilesFactor, aggregationMethod)
        if settings.ENABLE_TAGS:
          if not settings.SKIP_TAGS_FOR_NONTAGGED or ';' in metric:
            tagQueue.add(metric)
        instrumentation.increment('creates')
      except Exception as e:
        log.err()
        log.msg("Error creating %s: %s" % (metric, e))
        instrumentation.increment('errors')
        continue

    # now drain and persist some data
    (metric, datapoints) = cache.drain_metric()
    if metric is None:
      # end the loop
      break

    if not state.database.exists(metric):
      # If we get here, the metric must still be in new_metrics. We're
      # creating too fast, and we'll drop this data.
      instrumentation.increment('droppedCreates')
      continue

    # If we've got a rate limit configured lets makes sure we enforce it
    waitTime = 0
    if UPDATE_BUCKET:
      t1 = time.time()
      UPDATE_BUCKET.drain(1, blocking=True)
      waitTime = time.time() - t1

    try:
      t1 = time.time()
      # If we have duplicated points, always pick the last. update_many()
      # has no guaranteed behavior for that, and in fact the current implementation
      # will keep the first point in the list.
      datapoints = dict(datapoints).items()
      state.database.write(metric, datapoints)
      if settings.ENABLE_TAGS:
        if not settings.SKIP_TAGS_FOR_NONTAGGED or ';' in metric:
          tagQueue.update(metric)
      updateTime = time.time() - t1
    except Exception as e:
      log.err()
      log.msg("Error writing to %s: %s" % (metric, e))
      instrumentation.increment('errors')
    else:
      pointCount = len(datapoints)
      instrumentation.increment('committedPoints', pointCount)
      instrumentation.append('updateTimes', updateTime)
      if settings.LOG_UPDATES:
        if waitTime > 0.001:
          log.updates("wrote %d datapoints for %s in %.5f seconds after waiting %.5f seconds" % (
            pointCount, metric, updateTime, waitTime))
        else:
          log.updates("wrote %d datapoints for %s in %.5f seconds" % (
            pointCount, metric, updateTime))


def writeForever():
  while reactor.running:
    try:
      writeCachedDataPoints()
    except Exception:
      log.err()
      # Back-off on error to give the backend time to recover.
      time.sleep(0.1)
    else:
      # Avoid churning CPU when there are no metrics are in the cache
      time.sleep(1)


def writeTags():
  while True:
    tags = tagQueue.getbatch(settings.TAG_BATCH_SIZE)
    if not tags:
      break
    state.database.tag(*tags)


def writeTagsForever():
  while reactor.running:
    try:
      writeTags()
    except Exception:
      log.err()
      # Back-off on error to give the backend time to recover.
      time.sleep(0.1)
    else:
      # Avoid churning CPU when there are no series in the queue
      time.sleep(0.2)


def reloadStorageSchemas():
  global SCHEMAS
  try:
    SCHEMAS = loadStorageSchemas()
  except Exception as e:
    log.msg("Failed to reload storage SCHEMAS: %s" % (e))


def reloadAggregationSchemas():
  global AGGREGATION_SCHEMAS
  try:
    AGGREGATION_SCHEMAS = loadAggregationSchemas()
  except Exception as e:
    log.msg("Failed to reload aggregation SCHEMAS: %s" % (e))


def shutdownModifyUpdateSpeed():
    try:
        shut = settings.MAX_UPDATES_PER_SECOND_ON_SHUTDOWN
        if UPDATE_BUCKET:
          UPDATE_BUCKET.setCapacityAndFillRate(shut, shut)
        if CREATE_BUCKET:
          CREATE_BUCKET.setCapacityAndFillRate(shut, shut)
        log.msg("Carbon shutting down.  Changed the update rate to: " +
                str(settings.MAX_UPDATES_PER_SECOND_ON_SHUTDOWN))
    except KeyError:
        log.msg("Carbon shutting down.  Update rate not changed")

    # Also set MIN_TIMESTAMP_LAG to 0 to avoid waiting for nothing.
    settings.MIN_TIMESTAMP_LAG = 0


class WriterService(Service):

    def __init__(self):
        self.storage_reload_task = LoopingCall(reloadStorageSchemas)
        self.aggregation_reload_task = LoopingCall(reloadAggregationSchemas)

    def startService(self):
        if 'signal' in globals().keys():
          log.msg("Installing SIG_IGN for SIGHUP")
          signal.signal(signal.SIGHUP, signal.SIG_IGN)
        self.storage_reload_task.start(60, False)
        self.aggregation_reload_task.start(60, False)
        reactor.addSystemEventTrigger('before', 'shutdown', shutdownModifyUpdateSpeed)
        reactor.callInThread(writeForever)
        if settings.ENABLE_TAGS:
          reactor.callInThread(writeTagsForever)
        Service.startService(self)

    def stopService(self):
        self.storage_reload_task.stop()
        self.aggregation_reload_task.stop()
        Service.stopService(self)
