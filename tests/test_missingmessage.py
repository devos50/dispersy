from random import shuffle

from twisted.internet.defer import inlineCallbacks

from .dispersytestclass import DispersyTestFunc


class TestMissingMessage(DispersyTestFunc):
    @inlineCallbacks
    def _test_with_order(self, batchFUNC):
        """
        NODE generates a few messages and OTHER requests them one at a time.
        """
        node, other = self.create_nodes(2)
        yield node.send_identity(other)

        # create messages
        messages = [node.create_full_sync_text("Message #%d" % i, i + 10) for i in xrange(10)]
        yield node.give_messages(messages, node)

        batches = batchFUNC(messages)

        for messages in batches:
            global_times = sorted([message.distribution.global_time for message in messages])
            # request messages
            missing_message = yield other.create_missing_message(node.my_member, global_times)
            yield node.give_message(missing_message, other)

            # receive response
            responses = [response for _, response in other.receive_messages(names=[message.name])]
            self.assertEqual(sorted(response.distribution.global_time for response in responses), global_times)

    @inlineCallbacks
    def test_single_request(self):
        def batch(messages):
            return [[message] for message in messages]
        yield self._test_with_order(batch)

    @inlineCallbacks
    def test_single_request_out_of_order(self):
        def batch(messages):
            shuffle(messages)
            return [[message] for message in messages]
        yield self._test_with_order(batch)

    @inlineCallbacks
    def test_two_at_a_time(self):
        def batch(messages):
            batches = []
            for i in range(0, len(messages), 2):
                batches.append([messages[i], messages[i + 1]])
            return batches
        yield self._test_with_order(batch)
