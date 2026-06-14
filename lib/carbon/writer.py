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
from six.moves import queue

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


class SchemaMatchCache(object):
  """Memoize first-match schema selection for new metrics, keyed by metric name.

  Matching a new metric against the (potentially large) storage/aggregation schema
  lists is a per-metric regex scan and a hotspot when new metrics are dense. This
  caches the first schema that matches a given metric name so repeated lookups skip
  the scan.

  The cache is bound to the identity of the schema list it was built against. The
  reload tasks replace the SCHEMAS / AGGREGATION_SCHEMAS globals with brand new list
  objects on success (and leave them untouched on failure). Callers pass the current
  list into match(); when that list is a different object than the one the cache was
  built against, the cache is rebuilt. This makes invalidation atomic with the swap:
  a result computed against a now-stale list can never be served, and a failed reload
  (no swap) keeps the cache valid. invalidate() additionally drops everything eagerly
  so a successful reload frees the entries immediately rather than at the next write.

  Safe for the single writer thread populating the cache concurrently with
  reactor-thread invalidation: all access is guarded by a lock.
  """

  def __init__(self):
    self._lock = threading.Lock()
    self._schemas = None      # the schema list object this cache is bound to
    self._matches = {}        # metric name -> matched schema (or None for no match)

  def match(self, schemas, metric):
    with self._lock:
      if schemas is not self._schemas:
        # The schema list was swapped (a successful reload, or first use). Anything
        # cached was computed against a different list, so start fresh.
        self._schemas = schemas
        self._matches = {}
      try:
        # A cached None means "scanned, nothing matched" and must be honored.
        return self._matches[metric]
      except KeyError:
        pass
      matched = None
      for schema in schemas:
        if schema.matches(metric):
          matched = schema
          break
      self._matches[metric] = matched
      return matched

  def invalidate(self):
    with self._lock:
      self._schemas = None
      self._matches = {}


STORAGE_SCHEMA_CACHE = SchemaMatchCache()
AGGREGATION_SCHEMA_CACHE = SchemaMatchCache()


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
  def __init__(self, maxsize=0, update_interval=1):
    self.add_queue = queue.Queue(maxsize)
    self.update_queue = queue.Queue(maxsize)
    self.update_interval = update_interval
    self.update_counter = 0

  def add(self, metric):
    try:
      self.add_queue.put_nowait(metric)
    except queue.Full:
      pass

  def update(self, metric):
    self.update_counter = self.update_counter % self.update_interval + 1
    if self.update_counter == 1:
      try:
        self.update_queue.put_nowait(metric)
      except queue.Full:
        pass

  def getbatch(self, maxsize=1):
    batch = []
    while len(batch) < maxsize:
      try:
        batch.append(self.add_queue.get_nowait())
      except queue.Empty:
        break
    while len(batch) < maxsize:
      try:
        batch.append(self.update_queue.get_nowait())
      except queue.Empty:
        break
    return batch


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

      schema = STORAGE_SCHEMA_CACHE.match(SCHEMAS, metric)
      if schema is not None:
        if settings.LOG_CREATES:
          log.creates('new metric %s matched schema %s' % (metric, schema.name))
        archiveConfig = [archive.getTuple() for archive in schema.archives]

      aggregationSchema = AGGREGATION_SCHEMA_CACHE.match(AGGREGATION_SCHEMAS, metric)
      if aggregationSchema is not None:
        if settings.LOG_CREATES:
          log.creates('new metric %s matched aggregation schema %s'
                      % (metric, aggregationSchema.name))
        xFilesFactor, aggregationMethod = aggregationSchema.archives

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
  else:
    # Only invalidate on a successful reload; on failure SCHEMAS is unchanged and
    # the existing cache (built against it) is still correct.
    STORAGE_SCHEMA_CACHE.invalidate()


def reloadAggregationSchemas():
  global AGGREGATION_SCHEMAS
  try:
    AGGREGATION_SCHEMAS = loadAggregationSchemas()
  except Exception as e:
    log.msg("Failed to reload aggregation SCHEMAS: %s" % (e))
  else:
    # Only invalidate on a successful reload; see reloadStorageSchemas().
    AGGREGATION_SCHEMA_CACHE.invalidate()


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
