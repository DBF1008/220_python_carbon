import carbon.client as carbon_client
from carbon.client import (
  CarbonPickleClientFactory, CarbonPickleClientProtocol, CarbonLineClientProtocol,
  CarbonClientManager, RelayProcessor
)
from carbon.routers import DatapointRouter
from carbon.tests.util import TestSettings
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


class CarbonClientFactoryShutdownTest(TestCase):
  """Covers CarbonClientFactory.disconnect()'s shutdown lifecycle: stop
  receiving -> drain queue -> disconnect downstream -> finalize, with
  backpressure kept one-way (closing only) for the duration of shutdown.
  """
  timeout = 1.0

  def setUp(self):
    self.router_mock = Mock(spec=DatapointRouter)
    self.protocol_mock = Mock(spec=CarbonPickleClientProtocol)
    self.protocol_patch = patch(
      'carbon.client.CarbonPickleClientProtocol', new=Mock(return_value=self.protocol_mock))
    self.protocol_patch.start()
    # Mock the events module the factory talks to so we can assert exactly
    # which receiving-side events fire (and which are suppressed) during
    # shutdown, without mutating the process-global event handler lists.
    self.events_mock = Mock()
    self.events_patch = patch('carbon.client.state.events', new=self.events_mock)
    self.events_patch.start()
    carbon_client.settings = TestSettings()  # reset to defaults
    self.factories = []

  def tearDown(self):
    # Cancel any send still queued on the reactor to keep trial's reactor clean.
    for factory in self.factories:
      if factory.deferSendPending and factory.deferSendPending.active():
        factory.deferSendPending.cancel()
    self.events_patch.stop()
    self.protocol_patch.stop()

  def _make_factory(self, connected=False, started=False):
    factory = CarbonPickleClientFactory(('127.0.0.1', 2003, 'a'), self.router_mock)
    self.factories.append(factory)
    if connected:
      factory.buildProtocol(None)  # assigns the mocked protocol
      factory.connectedProtocol.connected = True
    factory.started = started
    return factory

  # --- scenario: not connected -------------------------------------------

  def test_disconnect_when_never_started_completes_immediately(self):
    factory = self._make_factory(connected=False, started=False)
    readyToStop = factory.disconnect()
    self.assertTrue(readyToStop.called)

  def test_disconnect_latches_shutdown_flag(self):
    factory = self._make_factory(connected=False, started=False)
    self.assertFalse(factory.shutdownInProgress)
    factory.disconnect()
    self.assertTrue(factory.shutdownInProgress)

  # --- scenario: connected, queue already empty --------------------------

  def test_disconnect_connected_empty_queue_disconnects_protocol(self):
    factory = self._make_factory(connected=True, started=True)
    self.assertFalse(factory.hasQueuedDatapoints())
    factory.disconnect()
    # Empty queue -> queueEmpty fires immediately -> stopConnecting drops it.
    self.protocol_mock.disconnect.assert_called_once_with()

  # --- scenario: stop receiving ------------------------------------------

  def test_disconnect_pauses_receiving(self):
    factory = self._make_factory(connected=True, started=True)
    factory.disconnect()
    self.events_mock.pauseReceivingMetrics.assert_called_once_with()

  # --- scenario: queue not empty -----------------------------------------

  def test_disconnect_with_queued_datapoints_waits_to_disconnect(self):
    factory = self._make_factory(connected=True, started=True)
    factory.enqueue('still.queued', (0, 1.0))
    readyToStop = factory.disconnect()
    # Queue is not empty: we must keep the connection up to drain it.
    self.assertFalse(self.protocol_mock.disconnect.called)
    self.assertFalse(readyToStop.called)
    # The drain must have been kicked so the queue can actually empty.
    self.assertIsNotNone(factory.deferSendPending)

  def test_disconnect_drops_connection_once_queue_drains(self):
    factory = self._make_factory(connected=True, started=True)
    factory.enqueue('still.queued', (0, 1.0))
    factory.disconnect()
    self.assertFalse(self.protocol_mock.disconnect.called)
    # Simulate the drain finishing.
    factory.queue.clear()
    factory.checkQueue()
    self.protocol_mock.disconnect.assert_called_once_with()

  # --- scenario: high priority metrics present ---------------------------

  def test_high_priority_datapoints_drained_before_regular_on_shutdown(self):
    # Use a real protocol so the drain order through the deque is exercised.
    self.protocol_patch.stop()
    try:
      carbon_client.settings.USE_RATIO_RESET = False
      factory = CarbonPickleClientFactory(('127.0.0.1', 2003, 'a'), self.router_mock)
      self.factories.append(factory)
      protocol = factory.buildProtocol(None)
      protocol.makeConnection(StringTransport())
      factory.started = True

      sent = []
      protocol._sendDatapointsNow = lambda datapoints: sent.extend(datapoints)

      factory.sendDatapoint('regular.1', (0, 1.0))
      factory.sendDatapoint('regular.2', (0, 2.0))
      # High priority goes to the head of the deque.
      factory.sendHighPriorityDatapoint('carbon.agents.high', (0, 9.0))

      factory.disconnect()
      # Drive the drain synchronously to completion.
      while factory.hasQueuedDatapoints():
        protocol.sendQueued()

      self.assertEqual(
        [metric for metric, _ in sent],
        ['carbon.agents.high', 'regular.1', 'regular.2'])
    finally:
      self.protocol_patch.start()  # keep tearDown's stop() balanced

  # --- one-way backpressure: resume must stay suppressed -----------------

  def test_drain_does_not_resume_receiving_during_shutdown(self):
    factory = self._make_factory(connected=True, started=True)
    factory.queueFull.callback(10000)  # pretend the queue had filled up
    factory.disconnect()
    self.events_mock.reset_mock()  # ignore pause/cacheFull fired so far
    # Draining below the low watermark would normally reopen reception.
    factory.queueSpaceCallback(0)
    self.events_mock.cacheSpaceAvailable.assert_not_called()

  def test_drain_resumes_receiving_when_not_shutting_down(self):
    # Control: outside shutdown the same drain DOES reopen reception.
    factory = self._make_factory(connected=True, started=True)
    factory.queueFull.callback(10000)
    factory.queueSpaceCallback(0)
    self.events_mock.cacheSpaceAvailable.assert_called_once_with()

  def test_late_connection_does_not_resume_receiving_during_shutdown(self):
    self.router_mock.hasDestination.return_value = False
    factory = self._make_factory(connected=True, started=True)
    factory.disconnect()
    self.events_mock.reset_mock()
    # A connection completing after shutdown began must not reopen reception.
    factory.destinationUp(('127.0.0.1', 2003, 'a'))
    self.events_mock.resumeReceivingMetrics.assert_not_called()

  def test_connection_up_resumes_receiving_when_not_shutting_down(self):
    # Control: outside shutdown a fresh destination DOES reopen reception.
    self.router_mock.hasDestination.return_value = False
    factory = self._make_factory(connected=True, started=True)
    factory.destinationUp(('127.0.0.1', 2003, 'a'))
    self.events_mock.resumeReceivingMetrics.assert_called_once_with()


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
