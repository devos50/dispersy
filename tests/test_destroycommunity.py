from twisted.internet.defer import inlineCallbacks

from .dispersytestclass import DispersyTestFunc


class TestDestroyCommunity(DispersyTestFunc):
    @inlineCallbacks
    def test_hard_kill(self):
        """
        Test that a community can be hard killed and their messages will be dropped from the DB.
        1. Node joins a community and sends a message.
        2. The message gets stored in the database.
        3. MM destroys the community.
        4. Node wipes all messages from the community in the database.
        """
        node, = self.create_nodes(1)

        message = node.create_full_sync_text("Should be removed", 42)
        yield node.give_message(message, node)

        node.assert_count(message, 1)

        dmessage = self._mm.create_destroy_community(u"hard-kill")

        yield node.give_message(dmessage, self._mm)

        node.assert_count(message, 0)

    @inlineCallbacks
    def test_hard_kill_without_permission(self):
        node, other = self.create_nodes(2)
        yield node.send_identity(other)

        message = node.create_full_sync_text("Should not be removed", 42)
        yield node.give_message(message, node)

        node.assert_count(message, 1)

        dmessage = other.create_destroy_community(u"hard-kill")
        yield node.give_message(dmessage, self._mm)

        node.assert_count(message, 1)
