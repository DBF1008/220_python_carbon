import carbon.client as carbon_client
from carbon.client import (
  CarbonPickleClientFactory, CarbonPickleClientProtocol, CarbonLineClientProtocol,
  CarbonClientManager, RelayProcessor
)
from carbon.routers import DatapointRouter
from carbon.tests.util import TestSettings
from carbon import instrumentation
import carbon.service  # NOQA

from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.base import DelayedCall
from twisted.internet.task import deferLater
from twisted.trial.unittest import TestCase
from twisted.test.proto_helpers import StringTransport

from mock import Mock, patch
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


class PooledReplicaConnectionQualityTest(TestCase):
  """connectionQualityMonitor() in DESTINATION_POOL_REPLICAS mode judges a
  connection by its own standing backlog (which replica selection cannot
  starve), while leaving the non-pooled metricsReceived ratio untouched.
  """

  def setUp(self):
    self.router_mock = Mock(spec=DatapointRouter)
    carbon_client.settings = TestSettings()  # reset to defaults
    carbon_client.settings.USE_RATIO_RESET = True
    self.factory = CarbonPickleClientFactory(('127.0.0.1', 2003, 'a'), self.router_mock)
    self.protocol = self.factory.buildProtocol(('127.0.0.1', 2003))
    # connectionMade() would normally derive these metric names; set them
    # directly so we can exercise the monitor without a live connection.
    self.protocol.sent = 'destinations.%s.sent' % self.factory.destinationName
    self.protocol.slowConnectionReset = (
        'destinations.%s.slowConnectionReset' % self.factory.destinationName)
    instrumentation.prior_stats.clear()

  def tearDown(self):
    instrumentation.prior_stats.clear()
    instrumentation.stats.clear()
    if self.factory.deferSendPending and self.factory.deferSendPending.active():
      self.factory.deferSendPending.cancel()

  def _set_backlog(self, n):
    self.factory.queue.clear()
    self.factory.queue.extend([('metric', (0, 0.0))] * n)

  def test_pooled_slow_replica_is_unhealthy(self):
    carbon_client.settings.DESTINATION_POOL_REPLICAS = True
    # Delivered 1000 last interval but 9000 are still queued: only 10% drained.
    instrumentation.prior_stats[self.protocol.sent] = 1000
    self._set_backlog(9000)
    self.assertFalse(self.protocol.connectionQualityMonitor())

  def test_pooled_recovering_replica_is_healthy(self):
    carbon_client.settings.DESTINATION_POOL_REPLICAS = True
    # Same connection after it caught up: small standing backlog, high delivery.
    instrumentation.prior_stats[self.protocol.sent] = 9500
    self._set_backlog(100)
    self.assertTrue(self.protocol.connectionQualityMonitor())

  def test_pooled_low_flow_is_not_reset(self):
    carbon_client.settings.DESTINATION_POOL_REPLICAS = True
    # delivered + backlog < MIN_RESET_STAT_FLOW (1000): too twitchy to act on.
    instrumentation.prior_stats[self.protocol.sent] = 100
    self._set_backlog(100)
    self.assertTrue(self.protocol.connectionQualityMonitor())

  def test_pooled_quality_ignores_starved_attempted_relays(self):
    # The masking scenario: selection routed traffic away from the slow replica
    # so attemptedRelays is tiny -- the OLD sent/attemptedRelays ratio saw that
    # as healthy. The backlog-aware check still flags the stalled connection.
    carbon_client.settings.DESTINATION_POOL_REPLICAS = True
    instrumentation.prior_stats[self.factory.attemptedRelays] = 50  # starved denom
    instrumentation.prior_stats[self.protocol.sent] = 1000
    self._set_backlog(9000)
    self.assertFalse(self.protocol.connectionQualityMonitor())

  def test_non_pooled_uses_metrics_received_and_ignores_backlog(self):
    carbon_client.settings.DESTINATION_POOL_REPLICAS = False
    # A huge backlog must NOT influence the non-pooled decision.
    self._set_backlog(100000)
    instrumentation.prior_stats[self.protocol.sent] = 9500
    instrumentation.prior_stats['metricsReceived'] = 10000
    # 9500 / 10000 = 0.95 >= 0.9 -> healthy, despite the backlog.
    self.assertTrue(self.protocol.connectionQualityMonitor())

  def test_non_pooled_below_ratio_is_unhealthy(self):
    carbon_client.settings.DESTINATION_POOL_REPLICAS = False
    self._set_backlog(0)
    instrumentation.prior_stats[self.protocol.sent] = 5000
    instrumentation.prior_stats['metricsReceived'] = 10000
    # 0.5 < 0.9 -> unhealthy (unchanged replication-mode behavior).
    self.assertFalse(self.protocol.connectionQualityMonitor())

  def test_non_pooled_low_flow_is_not_reset(self):
    carbon_client.settings.DESTINATION_POOL_REPLICAS = False
    instrumentation.prior_stats[self.protocol.sent] = 10
    instrumentation.prior_stats['metricsReceived'] = 100  # < 1000
    self.assertTrue(self.protocol.connectionQualityMonitor())

  def test_ratio_reset_disabled_is_always_healthy(self):
    carbon_client.settings.USE_RATIO_RESET = False
    carbon_client.settings.DESTINATION_POOL_REPLICAS = True
    instrumentation.prior_stats[self.protocol.sent] = 1
    self._set_backlog(100000)
    self.assertTrue(self.protocol.connectionQualityMonitor())


class PooledReplicaFactoryMetricsTest(TestCase):
  """updateSendRate() (per-connection throughput EWMA) and connectionCost()
  (backlog discounted by throughput) used for pooled replica selection.
  """

  def setUp(self):
    self.router_mock = Mock(spec=DatapointRouter)
    carbon_client.settings = TestSettings()
    self.factory = CarbonPickleClientFactory(('127.0.0.1', 2003, 'a'), self.router_mock)

  def tearDown(self):
    if self.factory.deferSendPending and self.factory.deferSendPending.active():
      self.factory.deferSendPending.cancel()

  def test_update_send_rate_folds_and_resets_window(self):
    self.factory.SEND_RATE_SMOOTHING = 0.5  # pin for determinism
    self.factory.relaySent = 10
    self.factory.relaySendRate = 0.0
    self.factory.updateSendRate()
    self.assertEqual(self.factory.relaySendRate, 5.0)  # 0.5*10 + 0.5*0
    self.assertEqual(self.factory.relaySent, 0)        # window reset
    # A subsequent idle window decays the rate toward zero.
    self.factory.updateSendRate()
    self.assertEqual(self.factory.relaySendRate, 2.5)

  def test_connection_cost_formula(self):
    self.factory.queue.extend([('m', (0, 0.0))] * 50)
    self.factory.relaySendRate = 99.0
    self.assertAlmostEqual(self.factory.connectionCost(), 50 / 100.0)

  def test_connection_cost_orders_by_queue_when_throughput_equal(self):
    a = CarbonPickleClientFactory(('h', 2003, 'a'), self.router_mock)
    b = CarbonPickleClientFactory(('h', 2003, 'b'), self.router_mock)
    a.relaySendRate = b.relaySendRate = 100.0
    a.queue.extend([('m', (0, 0.0))] * 10)
    b.queue.extend([('m', (0, 0.0))] * 30)
    self.assertLess(a.connectionCost(), b.connectionCost())

  def test_connection_cost_prefers_faster_drain_at_equal_queue(self):
    fast = CarbonPickleClientFactory(('h', 2003, 'a'), self.router_mock)
    slow = CarbonPickleClientFactory(('h', 2003, 'b'), self.router_mock)
    fast.queue.extend([('m', (0, 0.0))] * 50)
    slow.queue.extend([('m', (0, 0.0))] * 50)
    fast.relaySendRate = 200.0
    slow.relaySendRate = 0.0
    self.assertLess(fast.connectionCost(), slow.connectionCost())


class PooledReplicaSelectionTest(TestCase):
  """getFactories() in DESTINATION_POOL_REPLICAS mode picks the replica that can
  drain added work fastest (connectionCost), not merely the smallest queue.
  """
  timeout = 1.0

  def setUp(self):
    self.router_mock = Mock(spec=DatapointRouter)
    # Construct with pooling OFF so __init__ doesn't require the random resolver;
    # getFactories() reads the flag at call time, so we flip it per test.
    carbon_client.settings = TestSettings()
    self.client_mgr = CarbonClientManager(self.router_mock)

  def _make_replica(self, instance, queue_size, send_rate):
    factory = CarbonPickleClientFactory(('replica.host', 2003, instance), self.router_mock)
    factory.queue.extend([('m', (0, 0.0))] * queue_size)
    factory.relaySendRate = send_rate
    self.client_mgr.client_factories[factory.destination] = factory
    self.client_mgr.pooled_factories[('replica.host', 2003)].add(factory)
    return factory

  def _route_to(self, destination):
    # getFactories -> getDestinations -> list(router.getDestinations(metric)).
    # Use a list (re-consumable) so multiple getFactories calls keep working.
    self.router_mock.getDestinations.return_value = [destination]

  def test_balances_to_smallest_queue_when_throughput_equal(self):
    carbon_client.settings.DESTINATION_POOL_REPLICAS = True
    small = self._make_replica('a', queue_size=10, send_rate=100.0)
    self._make_replica('b', queue_size=20, send_rate=100.0)
    self._make_replica('c', queue_size=30, send_rate=100.0)
    self._route_to(('replica.host', 2003, 'a'))
    self.assertEqual(self.client_mgr.getFactories('some.metric'), {small})

  def test_avoids_stalled_replica_with_small_queue(self):
    carbon_client.settings.DESTINATION_POOL_REPLICAS = True
    # Stalled: tiny queue but zero throughput -> cost 5 / 1 = 5.
    self._make_replica('a', queue_size=5, send_rate=0.0)
    # Healthy: larger queue but draining fast -> cost 50 / 101 ~= 0.5.
    healthy = self._make_replica('b', queue_size=50, send_rate=100.0)
    self._route_to(('replica.host', 2003, 'a'))
    self.assertEqual(self.client_mgr.getFactories('some.metric'), {healthy})

  def test_recovered_replica_is_selected_again(self):
    carbon_client.settings.DESTINATION_POOL_REPLICAS = True
    recovering = self._make_replica('a', queue_size=5, send_rate=0.0)  # stalled
    healthy = self._make_replica('b', queue_size=50, send_rate=100.0)
    self._route_to(('replica.host', 2003, 'a'))
    # Initially the stalled replica is avoided.
    self.assertEqual(self.client_mgr.getFactories('some.metric'), {healthy})
    # It recovers: drains its backlog and its throughput climbs.
    recovering.queue.clear()
    recovering.relaySendRate = 200.0
    self.assertEqual(self.client_mgr.getFactories('some.metric'), {recovering})

  def test_non_pooled_selection_is_unchanged(self):
    carbon_client.settings.DESTINATION_POOL_REPLICAS = False
    d1 = ('host1', 2003, 'a')
    d2 = ('host2', 2003, 'a')
    f1 = Mock(spec=CarbonPickleClientFactory)
    f2 = Mock(spec=CarbonPickleClientFactory)
    self.client_mgr.client_factories[d1] = f1
    self.client_mgr.client_factories[d2] = f2
    self.router_mock.getDestinations.return_value = [d1, d2]
    # Broadcast semantics: every destination's factory is returned as-is.
    self.assertEqual(self.client_mgr.getFactories('some.metric'), {f1, f2})
