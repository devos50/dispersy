from time import time

from nose.twistedtools import reactor, deferred
from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import deferLater

from .dispersytestclass import DispersyTestFunc

class TestBatch(DispersyTestFunc):

    def __init__(self, *args, **kargs):
        super(TestBatch, self).__init__(*args, **kargs)
        self._big_batch_took = 0.0
        self._small_batches_took = 0.0


    @deferred(timeout=100000)
    @inlineCallbacks
    def test_bla(self):
        print "-------------------------"
        yield self.create_nodes()

    @inlineCallbacks
    def test_one_batch(self):
        print "here %s" % 1
        node, other = self.create_nodes(2)
        print "here %s" % 4
        yield other.send_identity(node)

        print "here %s" % 2

        messages = [node.create_batched_text("duplicates", i + 10) for i in range(10)]
        yield other.give_messages(messages, node, cache=True)

        print "here %s" % 3

        # no messages may be in the database, as they need to be batched
        other.assert_count(messages[0], 0)

        yield deferLater(reactor, messages[0].meta.batch.max_window + 1.0, lambda: None)

        # all of the messages must be stored in the database, as batch_window expired
        other.assert_count(messages[0], 10)

    @inlineCallbacks
    def test_multiple_batch(self):
        node, other = self.create_nodes(2)
        yield other.send_identity(node)

        messages = [node.create_batched_text("duplicates", i + 10) for i in range(10)]
        for message in messages:
            yield other.give_message(message, node, cache=True)

            # no messages may be in the database, as they need to be batched
            other.assert_count(message, 0)

        yield deferLater(reactor, messages[0].meta.batch.max_window + 1.0, lambda: None)

        # all of the messages must be stored in the database, as batch_window expired
        other.assert_count(messages[0], 10)

    @inlineCallbacks
    def test_one_big_batch(self, length=1000):
        """
        Test that one big batch of messages is processed correctly.
        Each community is handled in its own batch, hence we can measure performance differences when
        we make one large batch (using one community) and many small batches (using many different
        communities).
        """
        node, other = self.create_nodes(2)
        yield other.send_identity(node)

        messages = [node.create_full_sync_text("Dprint=False, big batch #%d" % global_time, global_time)
                    for global_time in xrange(10, 10 + length)]

        begin = time()
        yield other.give_messages(messages, node)
        end = time()
        self._big_batch_took = end - begin

        other.assert_count(messages[0], len(messages))

        if self._big_batch_took and self._small_batches_took:
            self.assertSmaller(self._big_batch_took, self._small_batches_took * 1.1)

    @inlineCallbacks
    def test_many_small_batches(self, length=1000):
        """
        Test that many small batches of messages are processed correctly.
        Each community is handled in its own batch, hence we can measure performance differences when
        we make one large batch (using one community) and many small batches (using many different
        communities).
        """
        node, other = self.create_nodes(2)
        yield other.send_identity(node)

        messages = [node.create_full_sync_text("Dprint=False, big batch #%d" % global_time, global_time)
                    for global_time in xrange(10, 10 + length)]

        begin = time()
        for message in messages:
            yield other.give_message(message, node)
        end = time()
        self._small_batches_took = end - begin

        other.assert_count(messages[0], len(messages))

        if self._big_batch_took and self._small_batches_took:
            self.assertSmaller(self._big_batch_took, self._small_batches_took * 1.1)
