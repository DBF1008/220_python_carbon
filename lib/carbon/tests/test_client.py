import carbon.client as carbon_client
from carbon.client import (
  CarbonPickleClientFactory, CarbonPickleClientProtocol, CarbonLineClientProtocol,
  CarbonClientManager, CarbonClientProtocol, RelayProcessor
)
from carbon.routers import DatapointRouter
from carbon.tests.util import TestSettings
import carbon.service  # NOQA

from time import time as _real_time
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.base import DelayedCall
from twisted.internet.task import deferLater
from twisted.trial.unittest import TestCase
from twisted.test.proto_helpers import StringTransport

from mock import Mock, patch, call
from pickle import loads as pickle_loads
from struct import unpack, calcsize


INT32_FORMAT = '!I'
INT32_SIZE = calcsize(INT32_FORMAT)


def decode_sent(data):
  pickle_size = unpack(INT32_FORMAT, data[:INT32_SIZE])[0]
  return pickle_loads(data[INT32_SIZE:INT32_SIZE + pickle_size])


class BroadcastRouter(DatapointRouter):
  def __init__(self, destinations=[]):
    self.destinations = set(destinations)

  def addDestination(self, destination):
    self.destinations.append(destination)

  def removeDestination(self, destination):
    self.destinations.discard(destination)

  def getDestinations(self, key):
    for destination in self.destinations:
      yield destination


class ConnectedCarbonClientProtocolTest(TestCase):
  def setUp(self):
    self.router_mock = Mock(spec=DatapointRouter)
    carbon_client.settings = TestSettings()  # reset to defaults
    factory = CarbonPickleClientFactory(('127.0.0.1', 2003, 'a'), self.router_mock)
    self.protocol = factory.buildProtocol(('127.0.0.1', 2003))
    self.transport = StringTransport()
    self.protocol.makeConnection(self.transport)

  def test_send_datapoint(self):
    def assert_sent():
      sent_data = self.transport.value()
      sent_datapoints = decode_sent(sent_data)
      self.assertEqual([datapoint], sent_datapoints)

    datapoint = ('foo.bar', (1000000000, 1.0))
    self.protocol.sendDatapoint(*datapoint)
    return deferLater(reactor, 0.1, assert_sent)


class CarbonLineClientProtocolTest(TestCase):
  def setUp(self):
    self.protocol = CarbonLineClientProtocol()
    self.protocol.sendLine = Mock()

  def test_send_datapoints(self):
    calls = [
      (('foo.bar', (1000000000, 1.0)), b'foo.bar 1 1000000000'),
      (('foo.bar', (1000000000, 1.1)), b'foo.bar 1.1 1000000000'),
      (('foo.bar', (1000000000, 1.123456789123)), b'foo.bar 1.1234567891 1000000000'),
      (('foo.bar', (1000000000, 1)), b'foo.bar 1 1000000000'),
      (('foo.bar', (1000000000, 1.498566361088E12)), b'foo.bar 1498566361088 1000000000'),
    ]

    i = 0
    for (datapoint, expected_line_to_send) in calls:
      i += 1

      self.protocol._sendDatapointsNow([datapoint])
      self.assertEqual(self.protocol.sendLine.call_count, i)
      self.protocol.sendLine.assert_called_with(expected_line_to_send)


class CarbonClientFactoryTest(TestCase):
  def setUp(self):
    self.router_mock = Mock(spec=DatapointRouter)
    self.protocol_mock = Mock(spec=CarbonPickleClientProtocol)
    self.protocol_patch = patch(
      'carbon.client.CarbonPickleClientProtocol', new=Mock(return_value=self.protocol_mock))
    self.protocol_patch.start()
    carbon_client.settings = TestSettings()
    self.factory = CarbonPickleClientFactory(('127.0.0.1', 2003, 'a'), self.router_mock)
    self.connected_factory = CarbonPickleClientFactory(('127.0.0.1', 2003, 'a'), self.router_mock)
    self.connected_factory.buildProtocol(None)
    self.connected_factory.started = True

  def tearDown(self):
    if self.factory.deferSendPending and self.factory.deferSendPending.active():
      self.factory.deferSendPending.cancel()
    self.protocol_patch.stop()

  def test_schedule_send_schedules_call_to_send_queued(self):
    self.factory.scheduleSend()
    self.assertIsInstance(self.factory.deferSendPending, DelayedCall)
    self.assertTrue(self.factory.deferSendPending.active())

  def test_schedule_send_ignores_already_scheduled(self):
    self.factory.scheduleSend()
    expected_fire_time = self.factory.deferSendPending.getTime()
    self.factory.scheduleSend()
    self.assertTrue(expected_fire_time, self.factory.deferSendPending.getTime())

  def test_send_queued_should_noop_if_not_connected(self):
    self.factory.scheduleSend()
    self.assertFalse(self.protocol_mock.sendQueued.called)

  def test_send_queued_should_call_protocol_send_queued(self):
    self.connected_factory.sendQueued()
    self.protocol_mock.sendQueued.assert_called_once_with()


class CarbonClientManagerTest(TestCase):
  timeout = 1.0

  def setUp(self):
    self.router_mock = Mock(spec=DatapointRouter)
    self.factory_mock = Mock(spec=CarbonPickleClientFactory)
    self.client_mgr = CarbonClientManager(self.router_mock)
    self.client_mgr.createFactory = lambda dest: self.factory_mock(dest, self.router_mock)

  def test_start_service_installs_sig_ignore(self):
    from signal import SIGHUP, SIG_IGN

    with patch('signal.signal', new=Mock()) as signal_mock:
      self.client_mgr.startService()
      signal_mock.assert_called_once_with(SIGHUP, SIG_IGN)

  def test_start_service_starts_factory_connect(self):
    factory_mock = Mock(spec=CarbonPickleClientFactory)
    factory_mock.started = False
    self.client_mgr.client_factories[('127.0.0.1', 2003, 'a')] = factory_mock
    self.client_mgr.startService()
    factory_mock.startConnecting.assert_called_once_with()

  def test_stop_service_waits_for_clients_to_disconnect(self):
    dest = ('127.0.0.1', 2003, 'a')
    self.client_mgr.startService()
    self.client_mgr.startClient(dest)

    disconnect_deferred = Deferred()
    reactor.callLater(0.1, disconnect_deferred.callback, 0)
    self.factory_mock.return_value.disconnect.return_value = disconnect_deferred
    return self.client_mgr.stopService()

  def test_start_client_instantiates_client_factory(self):
    dest = ('127.0.0.1', 2003, 'a')
    self.client_mgr.startClient(dest)
    self.factory_mock.assert_called_once_with(dest, self.router_mock)

  def test_start_client_ignores_duplicate(self):
    dest = ('127.0.0.1', 2003, 'a')
    self.client_mgr.startClient(dest)
    self.client_mgr.startClient(dest)
    self.factory_mock.assert_called_once_with(dest, self.router_mock)

  def test_start_client_starts_factory_if_running(self):
    dest = ('127.0.0.1', 2003, 'a')
    self.client_mgr.startService()
    self.client_mgr.startClient(dest)
    self.factory_mock.return_value.startConnecting.assert_called_once_with()

  def test_start_client_adds_destination_to_router(self):
    dest = ('127.0.0.1', 2003, 'a')
    self.client_mgr.startClient(dest)
    self.router_mock.addDestination.assert_called_once_with(dest)

  def test_stop_client_removes_destination_from_router(self):
    dest = ('127.0.0.1', 2003, 'a')
    self.client_mgr.startClient(dest)
    self.client_mgr.stopClient(dest)
    self.router_mock.removeDestination.assert_called_once_with(dest)


class RelayProcessorTest(TestCase):
  timeout = 1.0

  def setUp(self):
    carbon_client.settings = TestSettings()  # reset to defaults
    self.client_mgr_mock = Mock(spec=CarbonClientManager)
    self.client_mgr_patch = patch(
      'carbon.state.client_manager', new=self.client_mgr_mock)
    self.client_mgr_patch.start()

  def tearDown(self):
    self.client_mgr_patch.stop()

  def test_relay_normalized(self):
    carbon_client.settings.TAG_RELAY_NORMALIZED = True
    relayProcessor = RelayProcessor()
    relayProcessor.process('my.metric;foo=a;bar=b', (0.0, 0.0))
    self.client_mgr_mock.sendDatapoint.assert_called_once_with('my.metric;bar=b;foo=a', (0.0, 0.0))

  def test_relay_unnormalized(self):
    carbon_client.settings.TAG_RELAY_NORMALIZED = False
    relayProcessor = RelayProcessor()
    relayProcessor.process('my.metric;foo=a;bar=b', (0.0, 0.0))
    self.client_mgr_mock.sendDatapoint.assert_called_once_with('my.metric;foo=a;bar=b', (0.0, 0.0))


# ======================== New regression tests ========================


class _MockProto:
  """Lightweight protocol stub for scoring / quality tests."""

  def __init__(self, paused=False, connected=True):
    self.paused = paused
    self.connected = connected

  def disconnect(self):
    self.connected = False


class EffectiveQueueScoreTest(TestCase):
  """Tests for CarbonClientFactory.effectiveQueueScore()."""

  def setUp(self):
    self.router_mock = Mock(spec=DatapointRouter)
    carbon_client.settings = TestSettings()

  def _make_factory(self, dest=('127.0.0.1', 2003, 'a')):
    return CarbonPickleClientFactory(dest, self.router_mock)

  def test_active_connection_score(self):
    """Active, connected protocol → state_penalty=0."""
    factory = self._make_factory()
    factory.connectedProtocol = _MockProto(paused=False)
    for item in range(500):
      factory.queue.append(('m', (0, 0)))
    self.assertEqual(factory.effectiveQueueScore(), (0, 500))

  def test_paused_connection_score(self):
    """TCP-paused protocol → state_penalty=1."""
    factory = self._make_factory()
    factory.connectedProtocol = _MockProto(paused=True)
    for item in range(200):
      factory.queue.append(('m', (0, 0)))
    self.assertEqual(factory.effectiveQueueScore(), (1, 200))

  def test_disconnected_factory_score(self):
    """No protocol (disconnected) → state_penalty=2."""
    factory = self._make_factory()
    # connectedProtocol defaults to None
    self.assertEqual(factory.effectiveQueueScore(), (2, 0))

  def test_active_always_beats_paused(self):
    """Active connection with large queue beats paused with empty queue."""
    active = self._make_factory(('127.0.0.1', 2003, 'a'))
    active.connectedProtocol = _MockProto(paused=False)
    for _ in range(9999):
      active.queue.append(('m', (0, 0)))

    paused = self._make_factory(('127.0.0.1', 2003, 'b'))
    paused.connectedProtocol = _MockProto(paused=True)

    self.assertLess(active.effectiveQueueScore(), paused.effectiveQueueScore())

  def test_paused_beats_disconnected(self):
    """Paused connection beats disconnected factory."""
    paused = self._make_factory(('127.0.0.1', 2003, 'a'))
    paused.connectedProtocol = _MockProto(paused=True)
    for _ in range(9999):
      paused.queue.append(('m', (0, 0)))

    disconnected = self._make_factory(('127.0.0.1', 2003, 'b'))

    self.assertLess(paused.effectiveQueueScore(),
                    disconnected.effectiveQueueScore())


class QueueStallTrackingTest(TestCase):
  """Tests for queue stall detection in CarbonClientFactory."""

  def setUp(self):
    self.router_mock = Mock(spec=DatapointRouter)
    carbon_client.settings = TestSettings()
    self.factory = CarbonPickleClientFactory(
        ('127.0.0.1', 2003, 'a'), self.router_mock)
    self.factory.connectedProtocol = _MockProto()
    # stall_threshold = int(10000 * 0.5) = 5000

  def tearDown(self):
    if self.factory.deferSendPending and self.factory.deferSendPending.active():
      self.factory.deferSendPending.cancel()

  def _fill_queue(self, n):
    """Add n datapoints directly to the queue via sendDatapoint."""
    for i in range(n):
      self.factory.sendDatapoint('metric.%d' % i, (i, float(i)))

  def test_stall_starts_above_threshold(self):
    """_queueStallStart is set when queue exceeds stall threshold."""
    self.assertIsNone(self.factory._queueStallStart)
    self._fill_queue(5500)  # crosses 5000 threshold
    self.assertIsNotNone(self.factory._queueStallStart)

  def test_stall_not_started_below_threshold(self):
    """_queueStallStart stays None when queue is below threshold."""
    self._fill_queue(4999)  # stays below 5000
    self.assertIsNone(self.factory._queueStallStart)

  def test_stall_resets_below_threshold(self):
    """_queueStallStart is cleared when queue drains below threshold."""
    self._fill_queue(5500)
    self.assertIsNotNone(self.factory._queueStallStart)

    # Drain enough to drop below threshold (5000)
    while self.factory.queueSize > 4999:
      self.factory.takeSomeFromQueue()

    self.assertIsNone(self.factory._queueStallStart)
    self.assertEqual(self.factory._queueSizeAtStallStart, 0)

  def test_stall_duration_returns_zero_when_not_stalled(self):
    """queueStallDuration() returns 0 when no stall is tracked."""
    self._fill_queue(100)
    self.assertEqual(self.factory.queueStallDuration(), 0)

  def test_stall_duration_returns_elapsed_time(self):
    """queueStallDuration() returns wall-clock seconds since stall began."""
    self._fill_queue(5500)
    # Pin _queueStallStart to a known value
    self.factory._queueStallStart = 1000000.0
    self.factory._queueSizeAtStallStart = 5500

    with patch('carbon.client.time', return_value=1000200.0):
      duration = self.factory.queueStallDuration()
    self.assertAlmostEqual(duration, 200.0, places=0)

  def test_stall_resets_on_drain(self):
    """Stall timer resets when queue drains by more than 20%."""
    self._fill_queue(6000)
    self.factory._queueStallStart = 1000000.0
    self.factory._queueSizeAtStallStart = 6000

    # Drain 1500 items (25% of 6000) → triggers 20% drain detection
    for _ in range(3):
      self.factory.takeSomeFromQueue()  # 500 each = 1500 total

    # Queue = 4500, which is < 6000 * 0.8 = 4800 → drain detected
    # Queue = 4500 ≤ 5000 threshold → stall cleared
    self.assertIsNone(self.factory._queueStallStart)
    self.assertEqual(self.factory.queueStallDuration(), 0)

  def test_stall_tracked_when_paused(self):
    """Stall is tracked even when protocol is paused (TCP backpressure)."""
    self.factory.connectedProtocol = _MockProto(paused=True)
    self._fill_queue(5500)
    self.assertIsNotNone(self.factory._queueStallStart)


class PooledReplicaSelectionTest(TestCase):
  """Tests for getFactories() replica selection in pooled mode."""

  def setUp(self):
    self.router_mock = Mock(spec=DatapointRouter)
    self.router_mock.getDestinations = Mock()
    carbon_client.settings = TestSettings()
    # Create manager with POOL_REPLICAS=False first to avoid resolver setup
    self.manager = CarbonClientManager(self.router_mock)
    # Enable pooled mode AFTER construction
    carbon_client.settings.DESTINATION_POOL_REPLICAS = True

    self.dest_a = ('127.0.0.1', 2003, 'a')
    self.dest_b = ('127.0.0.1', 2003, 'b')
    self.dest_c = ('127.0.0.1', 2003, 'c')

    # Create real factories via manager
    self.factory_a = self.manager.createFactory(self.dest_a)
    self.factory_b = self.manager.createFactory(self.dest_b)
    self.factory_c = self.manager.createFactory(self.dest_c)

    self.manager.client_factories[self.dest_a] = self.factory_a
    self.manager.client_factories[self.dest_b] = self.factory_b
    self.manager.client_factories[self.dest_c] = self.factory_c

    self.manager.pooled_factories[('127.0.0.1', 2003)] = {
        self.factory_a, self.factory_b, self.factory_c}

    # All start with active, connected protocols
    for f in [self.factory_a, self.factory_b, self.factory_c]:
      f.connectedProtocol = _MockProto()

  def test_selects_smallest_queue_among_active(self):
    """Among active replicas, selects the one with the smallest queue."""
    for _ in range(100):
      self.factory_a.queue.append(('m', (0, 0)))
    for _ in range(50):
      self.factory_b.queue.append(('m', (0, 0)))
    for _ in range(200):
      self.factory_c.queue.append(('m', (0, 0)))

    self.router_mock.getDestinations.return_value = [self.dest_a]
    factories = self.manager.getFactories('test.metric')
    self.assertEqual(factories, {self.factory_b})  # smallest queue (50)

  def test_deprioritizes_paused_replica(self):
    """Paused replica is deprioritized even if it has a smaller queue."""
    for _ in range(100):
      self.factory_a.queue.append(('m', (0, 0)))
    for _ in range(10):
      self.factory_b.queue.append(('m', (0, 0)))
    for _ in range(200):
      self.factory_c.queue.append(('m', (0, 0)))
    self.factory_b.connectedProtocol.paused = True  # paused!

    self.router_mock.getDestinations.return_value = [self.dest_a]
    factories = self.manager.getFactories('test.metric')
    # Active factory_a (0, 100) beats paused factory_b (1, 10)
    # and active factory_c (0, 200). factory_a wins as smallest active.
    self.assertEqual(factories, {self.factory_a})

  def test_all_paused_selects_least_backlogged(self):
    """When all replicas are paused, selects the one with the smallest queue."""
    for _ in range(100):
      self.factory_a.queue.append(('m', (0, 0)))
    for _ in range(50):
      self.factory_b.queue.append(('m', (0, 0)))
    for _ in range(200):
      self.factory_c.queue.append(('m', (0, 0)))

    self.factory_a.connectedProtocol.paused = True
    self.factory_b.connectedProtocol.paused = True
    self.factory_c.connectedProtocol.paused = True

    self.router_mock.getDestinations.return_value = [self.dest_a]
    factories = self.manager.getFactories('test.metric')
    self.assertEqual(factories, {self.factory_b})  # smallest queue among paused

  def test_disconnected_replica_deprioritized(self):
    """Disconnected replica is not selected when active ones are available."""
    for _ in range(100):
      self.factory_a.queue.append(('m', (0, 0)))
    for _ in range(200):
      self.factory_b.queue.append(('m', (0, 0)))
    self.factory_c.connectedProtocol = None  # disconnected

    self.router_mock.getDestinations.return_value = [self.dest_a]
    factories = self.manager.getFactories('test.metric')
    # Active factory_a (0, 100) beats disconnected factory_c (2, 0)
    # and active factory_b (0, 200). factory_a wins as smallest active.
    self.assertEqual(factories, {self.factory_a})


class PooledConnectionQualityTest(TestCase):
  """Tests for connectionQualityMonitor() in pooled mode."""

  def setUp(self):
    self.router_mock = Mock(spec=DatapointRouter)
    carbon_client.settings = TestSettings()
    carbon_client.settings.USE_RATIO_RESET = True
    carbon_client.settings.DESTINATION_POOL_REPLICAS = True
    carbon_client.settings.MIN_RESET_STAT_FLOW = 1000
    carbon_client.settings.MIN_RESET_RATIO = 0.9
    carbon_client.settings.MIN_RESET_INTERVAL = 121

    self.factory = CarbonPickleClientFactory(
        ('127.0.0.1', 2003, 'a'), self.router_mock)
    self.protocol = CarbonPickleClientProtocol()
    self.protocol.factory = self.factory
    self.factory.connectedProtocol = self.protocol

    self.protocol.sent = 'destinations.127_0_0_1:2003:a.sent'
    self.protocol.slowConnectionReset = \
        'destinations.127_0_0_1:2003:a.slowConnectionReset'
    self.protocol.lastResetTime = _real_time()

  def _fill_queue(self, n):
    for i in range(n):
      self.factory.queue.append(('m', (i, float(i))))

  def test_disabled_returns_true(self):
    """Quality monitor returns True when USE_RATIO_RESET is disabled."""
    carbon_client.settings.USE_RATIO_RESET = False
    self.assertTrue(self.protocol.connectionQualityMonitor())

  def test_healthy_ratio_no_reset(self):
    """sent/attempted ratio above threshold → healthy (True)."""
    with patch.dict('carbon.instrumentation.prior_stats', {
        self.factory.attemptedRelays: 2000,
        self.protocol.sent: 1900,  # ratio = 0.95 > 0.9
    }):
      self.assertTrue(self.protocol.connectionQualityMonitor())

  def test_bad_ratio_triggers_reset(self):
    """sent/attempted ratio below threshold → unhealthy (False)."""
    with patch.dict('carbon.instrumentation.prior_stats', {
        self.factory.attemptedRelays: 2000,
        self.protocol.sent: 500,  # ratio = 0.25 < 0.9
    }):
      self.assertFalse(self.protocol.connectionQualityMonitor())

  def test_low_flow_no_stall_no_reset(self):
    """Low traffic + no queue stall → healthy (True)."""
    with patch.dict('carbon.instrumentation.prior_stats', {
        self.factory.attemptedRelays: 500,  # below MIN_RESET_STAT_FLOW
        self.protocol.sent: 100,
    }):
      # No stall tracked (queue is empty)
      self.assertTrue(self.protocol.connectionQualityMonitor())

  def test_starved_with_stalled_queue_triggers_reset(self):
    """Starved factory + stalled queue > MIN_RESET_INTERVAL → reset (False)."""
    # Manually set stall start to simulate a long-standing stall
    self.factory._queueStallStart = 1000000.0
    self.factory._queueSizeAtStallStart = 5500
    self._fill_queue(5500)

    with patch.dict('carbon.instrumentation.prior_stats', {
        self.factory.attemptedRelays: 100,  # starved: below MIN_RESET_STAT_FLOW
        self.protocol.sent: 0,
    }):
      with patch('carbon.client.time', return_value=1000200.0):
        # stall = 200s > MIN_RESET_INTERVAL=121, queue=5500 > threshold=5000
        self.assertFalse(self.protocol.connectionQualityMonitor())

  def test_starved_with_small_queue_no_reset(self):
    """Starved factory but queue below stall threshold → healthy (True)."""
    self.factory._queueStallStart = 1000000.0
    self.factory._queueSizeAtStallStart = 4000
    for _ in range(4000):
      self.factory.queue.append(('m', (0, 0)))

    with patch.dict('carbon.instrumentation.prior_stats', {
        self.factory.attemptedRelays: 100,
        self.protocol.sent: 0,
    }):
      with patch('carbon.client.time', return_value=1000200.0):
        # queue 4000 ≤ stall_threshold 5000 → no reset
        self.assertTrue(self.protocol.connectionQualityMonitor())

  def test_starved_recent_stall_no_reset(self):
    """Starved factory but stall duration < MIN_RESET_INTERVAL → healthy."""
    self.factory._queueStallStart = 1000000.0
    self.factory._queueSizeAtStallStart = 5500
    self._fill_queue(5500)

    with patch.dict('carbon.instrumentation.prior_stats', {
        self.factory.attemptedRelays: 100,
        self.protocol.sent: 0,
    }):
      with patch('carbon.client.time', return_value=1000060.0):
        # stall = 60s < MIN_RESET_INTERVAL=121 → no reset yet
        self.assertTrue(self.protocol.connectionQualityMonitor())

  def test_slow_replica_detected(self):
    """End-to-end: slow replica accumulates queue, gets detected by quality monitor.

    Simulates a factory that has been enqueuing data but sending very little.
    The attemptedRelays count is high (factory received traffic), but sent is
    low (connection can't keep up)."""
    # Simulate: factory received 3000 datapoints but only sent 300
    self._fill_queue(2700)  # queue holds the unsent backlog
    with patch.dict('carbon.instrumentation.prior_stats', {
        self.factory.attemptedRelays: 3000,  # high: traffic was routed here
        self.protocol.sent: 300,  # low: slow connection
    }):
      # ratio = 300/3000 = 0.1 < 0.9 → should trigger reset
      self.assertFalse(self.protocol.connectionQualityMonitor())

  def test_recovered_replica_reintegrates(self):
    """After queue drains (recovery), factory's score improves and it
    becomes competitive for selection again."""
    # Simulate: factory had a large stalled queue
    self.factory._queueStallStart = 1000000.0
    self.factory._queueSizeAtStallStart = 6000
    self._fill_queue(6000)

    self.assertEqual(self.factory.effectiveQueueScore(), (0, 6000))
    self.assertGreater(self.factory.queueStallDuration(), 0)

    # Recovery: queue drains completely
    self.factory.queue.clear()
    self.factory.takeSomeFromQueue()  # triggers stall reset

    # Score is now optimal
    self.assertEqual(self.factory.effectiveQueueScore(), (0, 0))
    self.assertEqual(self.factory.queueStallDuration(), 0)
    self.assertIsNone(self.factory._queueStallStart)


class NonPooledQualityTest(TestCase):
  """Tests ensuring non-pooled mode quality monitor is unchanged."""

  def setUp(self):
    self.router_mock = Mock(spec=DatapointRouter)
    carbon_client.settings = TestSettings()
    carbon_client.settings.USE_RATIO_RESET = True
    carbon_client.settings.DESTINATION_POOL_REPLICAS = False
    carbon_client.settings.MIN_RESET_STAT_FLOW = 1000
    carbon_client.settings.MIN_RESET_RATIO = 0.9

    self.factory = CarbonPickleClientFactory(
        ('127.0.0.1', 2003, 'a'), self.router_mock)
    self.protocol = CarbonPickleClientProtocol()
    self.protocol.factory = self.factory
    self.factory.connectedProtocol = self.protocol

    self.protocol.sent = 'destinations.127_0_0_1:2003:a.sent'
    self.protocol.slowConnectionReset = \
        'destinations.127_0_0_1:2003:a.slowConnectionReset'
    self.protocol.lastResetTime = _real_time()

  def test_non_pooled_uses_global_metrics_received(self):
    """Non-pooled mode uses global metricsReceived as denominator."""
    with patch.dict('carbon.instrumentation.prior_stats', {
        'metricsReceived': 10000,
        self.protocol.sent: 8000,  # ratio = 0.8 < 0.9
    }):
      self.assertFalse(self.protocol.connectionQualityMonitor())

  def test_non_pooled_healthy_ratio(self):
    """Non-pooled mode: good ratio → no reset."""
    with patch.dict('carbon.instrumentation.prior_stats', {
        'metricsReceived': 10000,
        self.protocol.sent: 9500,  # ratio = 0.95 > 0.9
    }):
      self.assertTrue(self.protocol.connectionQualityMonitor())

  def test_non_pooled_low_flow_returns_true(self):
    """Non-pooled mode: low global flow → can't judge → healthy."""
    with patch.dict('carbon.instrumentation.prior_stats', {
        'metricsReceived': 500,  # below MIN_RESET_STAT_FLOW
        self.protocol.sent: 100,
    }):
      self.assertTrue(self.protocol.connectionQualityMonitor())

  def test_non_pooled_ignores_stall_tracking(self):
    """Non-pooled mode does NOT use queue stall detection."""
    # Even with a stalled queue, non-pooled mode uses only the ratio check
    self.factory._queueStallStart = 1000000.0
    self.factory._queueSizeAtStallStart = 6000
    for _ in range(6000):
      self.factory.queue.append(('m', (0, 0)))

    with patch.dict('carbon.instrumentation.prior_stats', {
        'metricsReceived': 10000,
        self.protocol.sent: 9500,  # ratio = 0.95 → healthy
    }):
      # Despite stalled queue, ratio is good → no reset
      self.assertTrue(self.protocol.connectionQualityMonitor())


class ResetLoggingTest(TestCase):
  """Tests for resetConnectionForQualityReasons() logging."""

  def setUp(self):
    self.router_mock = Mock(spec=DatapointRouter)
    carbon_client.settings = TestSettings()
    carbon_client.settings.MIN_RESET_INTERVAL = 0  # no throttling for tests

    self.factory = CarbonPickleClientFactory(
        ('127.0.0.1', 2003, 'a'), self.router_mock)
    self.protocol = CarbonPickleClientProtocol()
    self.protocol.factory = self.factory
    self.factory.connectedProtocol = self.protocol
    self.protocol.connected = True
    self.protocol.transport = Mock()
    self.protocol.sent = 'destinations.127_0_0_1:2003:a.sent'
    self.protocol.slowConnectionReset = \
        'destinations.127_0_0_1:2003:a.slowConnectionReset'
    self.protocol.lastResetTime = 0  # allow immediate reset

  def test_pooled_reset_includes_queue_info(self):
    """Pooled mode reset log includes sent, attempted, queue, stall."""
    carbon_client.settings.DESTINATION_POOL_REPLICAS = True
    self.factory._queueStallStart = 1000000.0
    self.factory._queueSizeAtStallStart = 5500
    for _ in range(5500):
      self.factory.queue.append(('m', (0, 0)))

    with patch.dict('carbon.instrumentation.prior_stats', {
        self.factory.attemptedRelays: 100,
        self.protocol.sent: 10,
    }):
      with patch('carbon.client.time', return_value=1000200.0):
        with patch('carbon.client.log') as mock_log:
          self.protocol.resetConnectionForQualityReasons("test reason")
          # Verify log includes queue and stall info
          log_call_args = mock_log.clients.call_args[0][0]
          self.assertIn('queue=5500', log_call_args)
          self.assertIn('stall=', log_call_args)
          self.assertIn('test reason', log_call_args)

  def test_non_pooled_reset_simple_log(self):
    """Non-pooled mode reset log uses the simple format."""
    carbon_client.settings.DESTINATION_POOL_REPLICAS = False

    with patch('carbon.client.log') as mock_log:
      self.protocol.resetConnectionForQualityReasons("Sent: 100, Received: 200")
      log_call_args = mock_log.clients.call_args[0][0]
      self.assertIn('Sent: 100, Received: 200', log_call_args)
      self.assertNotIn('queue=', log_call_args)
