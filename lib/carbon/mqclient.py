from twisted.application.service import Service
from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.protocols.basic import Int32StringReceiver
from carbon.conf import settings
from carbon.util import pickle
from carbon import log, state, instrumentation

from txzmq import ZmqEndpoint, ZmqFactory, ZmqPubConnection, ZmqSubConnection

SEND_QUEUE_LOW_WATERMARK = settings.MAX_QUEUE_SIZE * 0.8

class CarbonMQClientManager(Service):
  def __init__(self, router):
    self.router = router

    print router
    self.client_factories = {} # { destination : CarbonClientFactory() }

  def startService(self):
    Service.startService(self)

  def stopService(self):
    Service.stopService(self)
    self.stopAllClients()

  def startClient(self, destination , method):
    if destination in self.client_factories:
      return

    log.clients("%s to zeromq endpont at %s" % (method,destination) )

    self.router.addDestination(destination)

    factory = ZmqFactory()

    endpoint = ZmqEndpoint(method, destination[0])

    self.client_factories[destination] = ZmqPubConnection(factory , endpoint)

    return

  def stopClient(self, destination):
    factory = self.client_factories.get(destination)
    if factory is None:
      return

    self.router.removeDestination(destination)

    stopCompleted = factory.shutdown()

    return

  def stopAllClients(self):
    deferreds = []
    for destination in list(self.client_factories):
      self.stopClient(destination)
    return

  def sendDatapoint(self, metric, datapoint):

    for destination in self.router.getDestinations(metric):
      self.client_factories[destination].publish('%f %d' % (datapoint[1],datapoint[0]), tag=metric)

  def __str__(self):
    return "<%s[%x]>" % (self.__class__.__name__, id(self))
