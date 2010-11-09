from Meta import MetaObject
from Payload import Permit

#
# Exceptions
#
class DelayPacket(Exception):
    """
    Raised by Conversion.decode_message when the packet can not be
    converted into a Message yet.  Delaying for 'some time' or until
    'some event' occurs.
    """
    pass

class DelayPacketByMissingMember(DelayPacket):
    """
    Raised during Conversion.decode_message when an unknown member id
    was received.  A member id is the sha1 hash over the member's
    public key, hence there is a small chance that members with
    different public keys will have the same member id.

    Raising this exception should result in a request for all public
    keys associated to the missing member id.
    """
    def __init__(self, missing_member_id):
        assert isinstance(missing_member_id, str)
        assert len(missing_member_id) == 20
        super(DelayPacketByMissingMember, self).__init__("Missing member")
        self._missing_member_id = missing_member_id

    @property
    def missing_member_id(self):
        return self._missing_member_id

class DelayPacketByUnspecifiedMember(DelayPacket):
    """
    Raised during Conversion.decode_message when an unknown member id
    was received.  A member id is the sha1 hash over the member's
    public key, hence there is a small chance that members with
    different public keys will have the same member id.

    Raising this exception should result in a request for all the
    member id / public key pairs that are part of the message.
    """
    def __init__(self, packet):
        assert isinstance(packet, str)
        super(DelayPacketByUnspecificMember, self).__init__("Unspecified member")
        self._packet = packet

    @property
    def packet(self):
        return self._packet
    
class DropPacket(Exception):
    """
    Raised by Conversion.decode_message when the packet is invalid.
    I.e. does not conform to valid syntax, contains malicious
    behaviour, etc.
    """
    pass

class DelayMessage(Exception):
    """
    Raised during Community.on_incoming_message or
    Community.on_incoming_dispersy_message (these call
    Community.on_message and Community.on_dispersy_message,
    respectively).  Delaying for 'some time' or until 'some event'
    occurs.
    """
    pass

class DelayMessageBySequence(DelayMessage):
    """
    Raised during Community.on_incoming_message or
    Community.on_incoming_dispersy_message (these call
    Community.on_message and Community.on_dispersy_message,
    respectively).

    Delaying until all missing sequence numbers have been received.  
    """
    def __init__(self, missing_low, missing_high):
        assert isinstance(missing_low, (int, long))
        assert isinstance(missing_high, (int, long))
        assert 0 < missing_low <= missing_high
        DelayMessage.__init__(self, "Missing sequence number(s)")
        self._missing_low = missing_low
        self._missing_high = missing_high

    @property
    def missing_low(self):
        return self._missing_low

    @property
    def missing_high(self):
        return self._missing_high

    def __str__(self):
        return "<{0.__class__.__name__} missing_low:{0.missing_low} missing_high:{0.missing_high}>".format(self)

class DelayMessageByProof(DelayMessage):
    """
    Raised during Community.on_incoming_message or
    Community.on_incoming_dispersy_message (these call
    Community.on_message and Community.on_dispersy_message,
    respectively).

    Delaying until a message is received that proves that the message
    is permitted.
    """
    def __init__(self):
        DelayMessage.__init__(self, "Missing proof")

class DropMessage(Exception):
    """
    Raised during Community.on_incoming_message or
    Community.on_incoming_dispersy_message (these call
    Community.on_message and Community.on_dispersy_message,
    respectively).

    Drops a message because it violates 'something'.  More specific
    reasons can be given with by raising a spectific subclass.
    """
    pass

class DropMessageByProof(DropMessage):
    """
    Raised during Community.on_incoming_message or
    Community.on_incoming_dispersy_message (these call
    Community.on_message and Community.on_dispersy_message,
    respectively).

    Drops a message because it violates a previously received message.
    This message should be provided to the origionator of this message
    to allow them to correct their mistake.
    """
    def __init__(self, message):
        DropMessage.__init__(self, "Provide proof")
        self._proof = message

    @property
    def proof(self):
        return self._proof

# #
# # Follow
# #
# class Follow(MetaObject):
#     class Implementation(object):
#         def __init__(self, meta):
#             self._meta = meta

#         @property
#         def meta(self):
#             return self._meta

# class NoFollow(Follow):
#     pass

# class RequestFollow(Follow):
#     class Implementation(Follow.Implementation):
#         def __init__(self, meta, identifier):
#             assert isinstance(identifier, str)
#             assert len(identifier) == 4
#             super(RequestFollow.Implementation, self).__init__(meta)
#             self._identifier = identifier

#         @property
#         def identifier(self):
#             return self._identifier

    # class Implementation(Follow.Implementation):
    #     def __init__(self, meta, identifier, handler=None, timeout=10.0, default=None):
    #         assert isinstance(meta, RequestFollow)
    #         assert hasattr(handler, "__call__")
    #         assert isinstance(timeout, float)
    #         assert default is None or isinstance(default, Message.Implementation)
    #         super(RequestFollow.Implementation, self).__init__(meta)
    #         self._identifier = identifier
    #         self._handler = handler
    #         self._timeout = timeout
    #         self._default = default

    #     @property
    #     def identifier(self):
    #         return self._identifier

    #     @property
    #     def handler(self):
    #         return self._handler

    #     @property
    #     def timeout(self):
    #         return self._timeout

    #     @property
    #     def default(self):
    #         return self._default

# class ResponseFollow(Follow):
#     class Implementation(Follow.Implementation):
#         def __init__(self, meta, identifier):
#             assert isinstance(identifier, str)
#             assert len(identifier) == 4
#             super(ResponseFollow.Implementation, self).__init__(meta)
#             self._identifier = identifier

#         @property
#         def identifier(self):
#             return self._identifier

#
# message
#
class Message(MetaObject):
    class Implementation(MetaObject.Implementation, Permit):
        def __init__(self, meta, authentication, distribution, destination, payload):
            if __debug__:
                from Authentication import Authentication
                from Destination import Destination
                from Distribution import Distribution
                from Payload import Authorize, Revoke
            assert isinstance(meta, Message), "META has invalid type '{0}'".format(type(meta))
            assert isinstance(authentication, Authentication.Implementation), "AUTHENTICATION has invalid type '{0}'".format(type(authentication))
            assert isinstance(distribution, Distribution.Implementation), "DISTRIBUTION has invalid type '{0}'".format(type(distribution))
            assert isinstance(destination, Destination.Implementation), "DESTINATION has invalid type '{0}'".format(type(destination))
            # assert isinstance(follow, Follow.Implementation), "FOLLOW has invalid type '{0}'".format(type(follow))
            assert isinstance(payload, (Permit, Authorize, Revoke)), "PAYLOAD has invalid type '{0}'".format(type(payload))
            super(Message.Implementation, self).__init__(meta)
            self._authentication = authentication
            self._distribution = distribution
            self._destination = destination
            # self._follow = follow
            self._payload = payload
            self._packet = ""

        @property
        def community(self):
            return self._meta._community

        @property
        def name(self):
            return self._meta._name

        @property
        def database_id(self):
            return self._meta._database_id

        @property
        def resolution(self):
            return self._meta._resolution

        @property
        def authentication(self):
            return self._authentication

        @property
        def distribution(self):
            return self._distribution

        @property
        def destination(self):
            return self._destination

        # @property
        # def follow(self):
        #     return self._follow
        
        @property
        def payload(self):
            return self._payload

        @property
        def packet(self):
            return self._packet

        @packet.setter
        def packet(self, packet):
            assert isinstance(packet, str)
            self._packet = packet

    def __init__(self, community, name, authentication, resolution, distribution, destination):
        if __debug__:
            from Community import Community
            from Authentication import Authentication
            from Resolution import Resolution
            from Destination import Destination
            from Distribution import Distribution
        assert isinstance(community, Community), "COMMUNITY has invalid type '{0}'".format(type(community))
        assert isinstance(name, unicode), "NAME has invalid type '{0}'".format(type(name))
        assert isinstance(authentication, Authentication), "AUTHENTICATION has invalid type '{0}'".format(type(authentication))
        assert isinstance(resolution, Resolution), "RESOLUTION has invalid type '{0}'".format(type(resolution))
        assert isinstance(distribution, Distribution), "DISTRIBUTION has invalid type '{0}'".format(type(distribution))
        assert isinstance(destination, Destination), "DESTINATION has invalid type '{0}'".format(type(destination))
        # assert isinstance(follow, Follow), "FOLLLOW has invalid type '{0}'".format(type(follow))
        assert self.check_policy_combination(authentication.__class__, resolution.__class__, distribution.__class__, destination.__class__)
        self._community = community
        self._name = name
        self._authentication = authentication
        self._resolution = resolution
        self._distribution = distribution
        self._destination = destination
        # self._follow = follow

    @property
    def community(self):
        return self._community

    @property
    def name(self):
        return self._name

    @property
    def authentication(self):
        return self._authentication

    @property
    def resolution(self):
        return self._resolution

    @property
    def distribution(self):
        return self._distribution

    @property
    def destination(self):
        return self._destination

    # @property
    # def follow(self):
    #     return self._callback

    @staticmethod
    def check_policy_combination(authentication, resolution, distribution, destination):
        from Authentication import Authentication, NoAuthentication, MemberAuthentication, MultiMemberAuthentication
        from Resolution import Resolution, PublicResolution, LinearResolution
        from Distribution import Distribution, RelayDistribution, DirectDistribution, FullSyncDistribution, LastSyncDistribution
        from Destination import Destination, AddressDestination, MemberDestination, CommunityDestination, SimilarityDestination

        assert issubclass(authentication, Authentication)
        assert issubclass(resolution, Resolution)
        assert issubclass(distribution, Distribution)
        assert issubclass(destination, Destination)

        def require(a, b, c):
            if not issubclass(b, c):
                raise ValueError("{0.__name__} does not support {1.__name__}".format(a, b))

        if issubclass(authentication, NoAuthentication):
            require(authentication, resolution, PublicResolution)
            require(authentication, distribution, (RelayDistribution, DirectDistribution))
            require(authentication, destination, (AddressDestination, MemberDestination, CommunityDestination))
        elif issubclass(authentication, MemberAuthentication):
            require(authentication, resolution, (PublicResolution, LinearResolution))
            require(authentication, distribution, (RelayDistribution, DirectDistribution, FullSyncDistribution, LastSyncDistribution))
            require(authentication, destination, (AddressDestination, MemberDestination, CommunityDestination, SimilarityDestination))
        elif issubclass(authentication, MultiMemberAuthentication):
            require(authentication, resolution, (PublicResolution, LinearResolution))
            require(authentication, distribution, (RelayDistribution, DirectDistribution, LastSyncDistribution))
            require(authentication, destination, (AddressDestination, MemberDestination, CommunityDestination, SimilarityDestination))
        else:
            raise ValueError("{0.__name__} is not supported".format(authentication))

        if issubclass(resolution, PublicResolution):
            require(resolution, authentication, (NoAuthentication, MemberAuthentication, MultiMemberAuthentication))
            require(resolution, distribution, (RelayDistribution, DirectDistribution, FullSyncDistribution, LastSyncDistribution))
            require(resolution, destination, (AddressDestination, MemberDestination, CommunityDestination, SimilarityDestination))
        elif issubclass(resolution, LinearResolution):
            require(resolution, authentication, (MemberAuthentication, MultiMemberAuthentication))
            require(resolution, distribution, (RelayDistribution, DirectDistribution, FullSyncDistribution, LastSyncDistribution))
            require(resolution, destination, (AddressDestination, MemberDestination, CommunityDestination, SimilarityDestination))
        else:
            raise ValueError("{0.__name__} is not supported".format(resolution))

        if issubclass(distribution, RelayDistribution):
            require(distribution, authentication, (NoAuthentication, MemberAuthentication, MultiMemberAuthentication))
            require(distribution, resolution, (PublicResolution, LinearResolution))
            require(distribution, destination, (AddressDestination, MemberDestination))
        elif issubclass(distribution, DirectDistribution):
            require(distribution, authentication, (NoAuthentication, MemberAuthentication, MultiMemberAuthentication))
            require(distribution, resolution, (PublicResolution, LinearResolution))
            require(distribution, destination, (AddressDestination, MemberDestination, CommunityDestination))
        elif issubclass(distribution, FullSyncDistribution):
            require(distribution, authentication, MemberAuthentication)
            require(distribution, resolution, (PublicResolution, LinearResolution))
            require(distribution, destination, (CommunityDestination, SimilarityDestination))
        elif issubclass(distribution, LastSyncDistribution):
            require(distribution, authentication, (MemberAuthentication, MultiMemberAuthentication))
            require(distribution, resolution, (PublicResolution, LinearResolution))
            require(distribution, destination, (CommunityDestination, SimilarityDestination))
        else:
            raise ValueError("{0.__name__} is not supported".format(distribution))
        
        if issubclass(destination, AddressDestination):
            require(destination, authentication, (NoAuthentication, MemberAuthentication, MultiMemberAuthentication))
            require(destination, resolution, (PublicResolution, LinearResolution))
            require(destination, distribution, (RelayDistribution, DirectDistribution))
        elif issubclass(destination, MemberDestination):
            require(destination, authentication, (NoAuthentication, MemberAuthentication, MultiMemberAuthentication))
            require(destination, resolution, (PublicResolution, LinearResolution))
            require(destination, distribution, (RelayDistribution, DirectDistribution))
        elif issubclass(destination, CommunityDestination):
            require(destination, authentication, (NoAuthentication, MemberAuthentication, MultiMemberAuthentication))
            require(destination, resolution, (PublicResolution, LinearResolution))
            require(destination, distribution, (DirectDistribution, FullSyncDistribution, LastSyncDistribution))
        elif issubclass(destination, SimilarityDestination):
            require(destination, authentication, (MemberAuthentication, MultiMemberAuthentication))
            require(destination, resolution, (PublicResolution, LinearResolution))
            require(destination, distribution, (FullSyncDistribution, LastSyncDistribution))
        else:
            raise ValueError("{0.__name__} is not supported".format(destination))

        return True
