import os
import logging
import sys
from unittest import TestCase
from tempfile import mkdtemp

# Do not (re)move the reactor import, even if we aren't using it
# (nose starts the reactor in a separate thread when importing this)\
from twisted.python.threadable import isInIOThread

if "twisted.internet.reactor" in sys.modules.keys():
    """ Tribler already imported the reactor. """
    from twisted.internet import reactor
else:
    from nose.twistedtools import reactor

from twisted.internet.defer import inlineCallbacks, returnValue

from ..discovery.community import PEERCACHE_FILENAME
from ..dispersy import Dispersy
from ..endpoint import ManualEnpoint
from ..util import blockingCallFromThread
from .debugcommunity.community import DebugCommunity
from .debugcommunity.node import DebugNode


# use logger.conf if it exists
if os.path.exists("logger.conf"):
    # will raise an exception when logger.conf is malformed
    logging.basicConfig(filename="logger.conf")
# fallback to basic configuration when needed
logging.basicConfig(format="%(asctime)-15s [%(levelname)s] %(message)s")


class DispersyTestFunc(TestCase):

    def __init__(self, *args, **kwargs):
        super(DispersyTestFunc, self).__init__(*args, **kwargs)
        self._logger = logging.getLogger(self.__class__.__name__)

    def on_callback_exception(self, exception, is_fatal):
        return True

    def setUp(self):
        super(DispersyTestFunc, self).setUp()

        self.dispersy_objects = []

        self.assertFalse(reactor.getDelayedCalls())
        """" Central node that is also used for master member. """
        self._mm = None
        self._mm, = self.create_nodes()

        print self._mm._community

        self._dispersy = self._mm._dispersy
        self._community = self._mm._community

    def tearDown(self):
        super(DispersyTestFunc, self).tearDown()

        for dispersy in self.dispersy_objects:
            dispersy.stop()

            peercache = os.path.join(dispersy._working_directory, PEERCACHE_FILENAME)
            if os.path.isfile(peercache):
                os.unlink(peercache)

        pending = reactor.getDelayedCalls()
        if pending:
            self._logger.warning("Found delayed calls in reactor:")
            for dc in pending:
                fun = dc.func
                self._logger.warning("    %s", fun)
            self._logger.warning("Failing")
        assert not pending, "The reactor was not clean after shutting down all dispersy instances."

    def create_nodes(self, amount=1, store_identity=True, tunnel=False, community_class=DebugCommunity,
                     autoload_discovery=False, memory_database=True):
        """
        Creates dispersy nodes running a community.
        :param amount: The amount of nodes that need to be created.
        :param store_identity: If the identity is send to the central node.
        :param tunnel: If the nodes is behind a tunnel or not
        :param community_class: The class that the node will autoload.
        :param autoload_discovery: If the discovery community is autoloaded.
        :param memory_database: If a memory database is used.
        :return: [(DebugNode)]
        """

        """ Override this method in a subclass with a different community class to test communities. """

        @inlineCallbacks
        def _create_nodes(amount, store_identity, tunnel, communityclass, autoload_discovery, memory_database):
            nodes = []
            for _ in range(amount):
                # TODO(emilon): do the log observer stuff instead
                # callback.attach_exception_handler(self.on_callback_exception)
                print "test %s" % 1
                memory_database_argument = {'database_filename': u":memory:"} if memory_database else {}
                working_directory = unicode(mkdtemp(suffix="_dispersy_test_session"))

                print "test %s" % 2
                dispersy = Dispersy(ManualEnpoint(0), working_directory, **memory_database_argument)
                print "test %s" % 3
                yield dispersy.initialize_statistics()
                print "test %s" % 4
                yield dispersy.start(autoload_discovery=autoload_discovery)
                print "test %s" % 5

                self.dispersy_objects.append(dispersy)

                node = yield self._create_node(dispersy, communityclass, self._mm)
                print "test %s" % 6
                yield node.init_my_member(tunnel=tunnel, store_identity=store_identity)
                print "test %s" % 7

                nodes.append(node)
            self._logger.debug("create_nodes, nodes created: %s", nodes)
            returnValue(nodes)

        return blockingCallFromThread(reactor, _create_nodes, amount, store_identity, tunnel, community_class,
                                      autoload_discovery, memory_database)

    @inlineCallbacks
    def _create_node(self, dispersy, community_class, c_master_member):
        node = DebugNode(self, dispersy)
        yield node.initialize(community_class, c_master_member)
        returnValue(node)
