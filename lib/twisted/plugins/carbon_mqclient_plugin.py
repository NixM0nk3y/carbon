from zope.interface import implements

from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker

from carbon import service
from carbon import conf


class CarbonMQServiceMaker(object):

    implements(IServiceMaker, IPlugin)
    tapname = "carbon-mqclient"
    description = "Relay stats for graphite into MQ."
    options = conf.CarbonMQOptions

    def makeService(self, options):
        """
        Construct a C{carbon-relay} service.
        """
        return service.createMQService(options)


# Now construct an object which *provides* the relevant interfaces
serviceMaker = CarbonMQServiceMaker()
