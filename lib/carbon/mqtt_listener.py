#!/usr/bin/env python
"""
Copyright 2009 Lucio Torre <lucio.torre@canonical.com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

This is an MQTT client that will connect to the specified broker and read
messages, parse them, and post them as metrics.

Each message's routing key should be a metric name.
The message body should be one or more lines of the form:

<value> <timestamp>\n
<value> <timestamp>\n
...

Where each <value> is a real number and <timestamp> is a UNIX epoch time.


This program can be started standalone for testing or using carbon-cache.py
(see example config file provided)
"""
import sys
import os
import socket
import random
import json
from optparse import OptionParser
from string import maketrans

from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.application.internet import TCPClient

from MQTT import MQTTProtocol, MQTTClient

try:
    import carbon
except ImportError:
    # this is being run directly, carbon is not installed
    LIB_DIR = os.path.dirname(os.path.dirname(__file__))
    sys.path.insert(0, LIB_DIR)

import carbon.protocols  # NOQA satisfy import order requirements
from carbon.protocols import CarbonServerProtocol
from carbon.conf import settings
from carbon import log, events

HOSTNAME = socket.gethostname().split('.')[0]

log.logToStdout()

class MQTTProtocol(CarbonServerProtocol):
    plugin_name = "mqtt"

    @classmethod
    def build(cls, root_service):
        if not settings.ENABLE_MQTT:
            return

        mqtt_host = settings.get("MQTT_HOST", "localhost")
        mqtt_port = settings.get("MQTT_PORT", 1883)
        mqtt_user = settings.get("MQTT_USER", "guest")
        mqtt_password = settings.get("MQTT_PASSWORD", "guest")
        mqtt_verbose  = settings.get("MQTT_VERBOSE", False)

        factory = createMQTTListener(
            mqtt_user, mqtt_password,
            verbose=mqtt_verbose)
        service = TCPClient(mqtt_host, int(mqtt_port), factory)
        service.setServiceParent(root_service)

class MQTTGraphiteProtocol(MQTTClient):
    """This is the protocol instance that will receive and post metrics."""

    def __init__(self, clientId=None, keepalive=None, willQos=0,
                 willTopic=None, willMessage=None, willRetain=False, messagepoll=None, verbose = False):

        if clientId is not None:
            self.clientId = clientId
        else:
            self.clientId = "Twisted%i" % random.randint(1, 0xFFFF)

        if keepalive is not None:
            self.keepalive = keepalive
        else:
            self.keepalive = 3000

        if messagepoll is not None:
            self.MessagesLoop = messagepoll
        else:
            self.MessagesLoop = 5

        self.willQos = willQos
        self.willTopic = willTopic
        self.willMessage = willMessage
        self.willRetain = willRetain
        self.verbose = verbose

    @inlineCallbacks
    def connectionMade(self):
        if self.verbose:
            log.listener("New MQTT connection made")

        self.PingLoop = reactor.callLater(self.keepalive//1000, self.pingreq)
        self.ProcessMessagesLoop = reactor.callLater(self.MessagesLoop, self.processMessages)

        yield self.connect(self.clientId, self.keepalive, self.willTopic,
                     self.willMessage, self.willQos, self.willRetain, True)

    @inlineCallbacks
    def pingrespReceived(self):
        if self.verbose: 
            log.listener('Ping received from MQTT broker')
        yield reactor.callLater(self.keepalive//1000, self.pingreq)

    @inlineCallbacks
    def connackReceived(self, status):

        if status == 0:
            # bind each configured metric pattern
            for bind_pattern in settings.BIND_PATTERNS:
                log.listener("binding with pattern %s" \
                         % (bind_pattern))
                yield self.subscribe( topic=bind_pattern )

        else:
            log.listener('Connecting to MQTT broker failed')

    @inlineCallbacks
    def publishReceived(self, topic, message, qos, dup, retain, messageId):

        # process the message
        yield self.processMessage(topic, message)

    @inlineCallbacks
    def processMessages(self):
        yield reactor.callLater(self.MessagesLoop, self.processMessages)

    def processMessage(self, topic, message):
        """Parse a message and post it as a metric."""

        if self.verbose:
            log.listener('RECV Topic: %s, Message: %s' % (topic, message))

        # translate topic into metric name
        trans = maketrans('/','.')
        metric = topic.translate(trans)

        for line in message.split("\n"):
            line = line.strip()

            if not line:
                continue
            try:
                value = None
                timestamp = None

                if settings.get("MQTT_METRIC_NAME_IN_BODY", False):
                    metric, value, timestamp = line.split()
                else:
                    value, timestamp = line.split()

                datapoint = ( float(timestamp), float(value) )

            except ValueError:
                log.listener("invalid message line: %s" % (line,))
                continue

            events.metricReceived(metric, datapoint)

            if self.verbose:
                log.listener("Metric posted: %s %s %s" %
                             (metric, value, timestamp,))


class MQTTListenerFactory(ReconnectingClientFactory):
    """
        The connecting factory.


    """
    def __init__(self, username, password, verbose, service = None):
        self.username = username
        self.password = password
        self.verbose = verbose
        self.service = service
        self.protocol = MQTTGraphiteProtocol

    def clientConnectionFailed(self, connector, reason):
        log.listener('CAUGHT In The ACT: Connection failed. Reason: %s' % (reason))
        ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)

    def buildProtocol(self, addr):
        self.resetDelay()
        p = self.protocol()
        p.factory = self
        return p

def createMQTTListener(username, password, verbose=False):
    """
    Create an MQTTListenerFactory configured with the specified options.
    """

    factory = MQTTListenerFactory(username, password, verbose=verbose)

    return factory


def startReceiver(host, port, username, password, verbose=False):
    """
    Starts a twisted process that will read messages on the mqtt broker and
    post them as metrics.
    """
    factory = createMQTTListener(username, password, verbose=verbose)

    reactor.connectTCP(host, port, factory)

def main():
    parser = OptionParser()
    parser.add_option("-t", "--host", dest="host",
                      help="host name", metavar="HOST", default="localhost")

    parser.add_option("-p", "--port", dest="port", type=int,
                      help="port number", metavar="PORT",
                      default=1883)

    parser.add_option("-u", "--user", dest="username",
                      help="username", metavar="USERNAME",
                      default="guest")

    parser.add_option("-w", "--password", dest="password",
                      help="password", metavar="PASSWORD",
                      default="guest")

    parser.add_option("-v", "--verbose", dest="verbose",
                      help="verbose",
                      default=False, action="store_true")

    (options, args) = parser.parse_args()


    startReceiver(options.host, options.port, options.username,
                  options.password, verbose=options.verbose)

    reactor.run()

if __name__ == "__main__":
    main()
