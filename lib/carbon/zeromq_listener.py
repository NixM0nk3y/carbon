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

This is an ZeroMQ client that will connect/bind to the specified endpoint and read
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

from twisted.internet import reactor, defer
from twisted.application.service import Service, Application

from txzmq import ZmqEndpoint, ZmqFactory, ZmqPubConnection, ZmqSubConnection

try:
    import carbon
except:
    # this is being run directly, carbon is not installed
    LIB_DIR = os.path.dirname(os.path.dirname(__file__))
    sys.path.insert(0, LIB_DIR)

    from carbon.conf import settings
    settings['CONF_DIR']='/opt/graphite/conf'

import carbon.protocols #satisfy import order requirements
from carbon.conf import settings
from carbon import log, events, instrumentation

HOSTNAME = socket.gethostname().split('.')[0]

log.logToStdout()

class ZeroMQProcessor(Service):

    """
       foo
    """

    def __init__(self, method, endpoint, topics, verbose = False):

        self.verbose = verbose
        self.method = method
        self.endpoint = endpoint
        self.topics = topics

        log.listener("%s 0MZ on %s" % ( self.method, self.endpoint) )

    def startService(self):
        """

        """

        mqfactory = ZmqFactory()
        mqendpoint = ZmqEndpoint(self.method, self.endpoint)

        s = ZmqSubConnection(mqfactory, mqendpoint)

        # default to all topics if non specified
        if not len(self.topics):
            self.topics.append('')

        for topic in self.topics:

            log.listener("Subscribing to topic: %s" % topic)

            s.subscribe(topic)

        s.gotMessage = self.processMessage

        log.listener("0MZ listener setup complete !")


    def processMessage( self,  *args ):
        """Parse a message and post it as a metric."""

        payload = ''
        topic = ''
        if len(args) == 2:
            [ payload , topic ] = args
        else:
            paypal = args[0]

        if self.verbose:
            log.listener('RECV Topic: %s, Message: %s' % (topic, payload))

        # translate topic into metric name
        trans = maketrans('/','.')
        metric = topic.translate(trans)

        for line in payload.split("\n"):
            line = line.strip()

            if not line:
                continue
            try:
                value = None
                timestamp = None

                if settings.get("MQ_METRIC_NAME_IN_BODY", False):
                    metric, value, timestamp = line.split()
                else:
                    value, timestamp = line.split()

                datapoint = ( float(timestamp), float(value) )

            except ValueError:
                log.listener("invalid message line: %s" % (line,))
                continue

            #events.metricReceived(metric, datapoint)

            if self.verbose:
               log.listener("Metric posted: %s %s %s" %
                            (metric, value, timestamp,))

def startReceiver(method, endpoint, topics, verbose=False):
    """
    Starts a twisted process that will read messages from the 0MQ endpoint and
    post them as metrics.
    """

    zmqservice = ZeroMQProcessor( method, endpoint, topics, verbose )

    application = Application("zeromq") 

    zmqservice.setServiceParent(application)

    from twisted.application import app
    app.startApplication(application, False)

    #Service.startService(zmqservice)


def main():
    parser = OptionParser()
    parser.add_option("-m", "--method", dest="method",
                      help="Method - (connect|bind)", metavar="METHOD", default="bind")

    parser.add_option("-e", "--endpoint", dest="endpoint",
                      help="Endpoint", metavar="ENDPOINT", default="ipc:///tmp/graphite.sock")

    parser.add_option("-t", "--topic", dest="topic",
                      help="Topic", metavar="TOPIC", default=[], action='append')

    parser.add_option("-v", "--verbose", dest="verbose",
                      help="verbose",
                      default=False, action="store_true")

    (options, args) = parser.parse_args()


    startReceiver(options.method, options.endpoint, options.topic, verbose=options.verbose)

    reactor.run()

if __name__ == "__main__":
    main()
