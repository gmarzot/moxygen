# moqsession.pyx

from libcpp.memory cimport shared_ptr, make_shared, unique_ptr
from libcpp.utility cimport pair
from cython.operator cimport dereference as deref
from libc.stdint cimport uint64_t

# Basic Python class for setup parameters
class SetupParams:
    def __init__(self, max_subscribe_id: int = 100):
        self.max_subscribe_id = max_subscribe_id

cdef extern from "proxygen/lib/http/webtransport/WebTransport.h" namespace "proxygen":
    cdef cppclass WebTransport:
        pass

cdef extern from "moxygen/MoQSession.h" namespace "moxygen":
    cdef cppclass MoQSession:
        enum Direction:
            CLIENT
            SERVER
            
        MoQSession(Direction dir, WebTransport* wt, EventBase* evb) except +
        void start()
        void close()
        void setup(ClientSetup setup)
        void setup(ServerSetup setup)
        
        # We'll let folly::coro::Task handle its business internally
        Task[void] setupComplete()
        Task[Expected[shared_ptr[TrackHandle], SubscribeError]] subscribe(SubscribeRequest)

cdef extern from "proxygen/lib/http/webtransport/test/FakeSharedWebTransport.h" namespace "proxygen::test":
    cdef cppclass FakeSharedWebTransport:
        @staticmethod
        pair[unique_ptr[FakeSharedWebTransport], unique_ptr[FakeSharedWebTransport]] makeSharedWebTransport()

cdef class MoqSession:
    cdef shared_ptr[MoQSession] _session
    cdef shared_ptr[EventBase] _evb
    
    @staticmethod
    def create_session_pair():
        """Create a pair of connected client and server sessions with a shared event base"""
        cdef shared_ptr[EventBase] evb = make_shared[EventBase]()
        cdef auto transport_pair = FakeSharedWebTransport.makeSharedWebTransport()
        
        cdef MoqSession client = MoqSession.__new__(MoqSession)
        cdef MoqSession server = MoqSession.__new__(MoqSession)
        
        client._evb = evb
        server._evb = evb
        
        client._session = make_shared[MoQSession](
            MoQSession.Direction.CLIENT,
            transport_pair.first.get(),
            evb.get()
        )
        
        server._session = make_shared[MoQSession](
            MoQSession.Direction.SERVER, 
            transport_pair.second.get(),
            evb.get()
        )
        
        transport_pair.first.setPeerHandler(server._session.get())
        transport_pair.second.setPeerHandler(client._session.get())
        
        return client, server
    
    def start(self):
        """Start the session"""
        self._session.get().start()
        
    def close(self):
        """Close the session"""
        self._session.get().close()
        
    def run_event_base(self):
        """Run the event base - blocks until complete"""
        self._evb.get().loop()
        
    def run_one_event(self):
        """Process one event"""
        self._evb.get().loopOnce()
        
    def setup_client(self, params: SetupParams):
        """Setup client session with parameters"""
        cdef ClientSetup setup
        setup.supportedVersions = [kVersionDraftCurrent]
        setup.params = [
            {
                .key = folly.to_underlying(SetupKey.MAX_SUBSCRIBE_ID),
                .asUint64 = params.max_subscribe_id
            }
        ]
        self._session.get().setup(setup)
        
    def setup_server(self, params: SetupParams):
        """Setup server session with parameters"""
        cdef ServerSetup setup 
        setup.selectedVersion = kVersionDraftCurrent
        setup.params = [
            {
                .key = folly.to_underlying(SetupKey.MAX_SUBSCRIBE_ID),
                .asUint64 = params.max_subscribe_id
            }
        ]
        self._session.get().setup(setup)

    cdef _handle_subscribe_error(self, const SubscribeError& error):
        """Convert C++ SubscribeError to Python dict"""
        return {
            'subscribe_id': error.subscribeID,
            'error_code': SubscribeErrorCode(error.errorCode),  # Convert to enum
            'reason': error.reasonPhrase.decode('utf-8'),
            'retry_alias': deref(error.retryAlias) if error.retryAlias.has_value() else None
        }

    cdef _handle_absolute_location(self, const AbsoluteLocation& loc):
        """Convert C++ AbsoluteLocation to Python dict"""
        return {
            'group': loc.group,
            'object': loc.object
        }

    cdef _handle_subscribe_done(self, const SubscribeDone& done):
        """Convert C++ SubscribeDone to Python dict"""
        result = {
            'subscribe_id': done.subscribeID,
            'status_code': SubscribeDoneStatusCode(done.statusCode),
            'reason': done.reasonPhrase.decode('utf-8'),
        }
        if done.finalObject.has_value():
            result['final_object'] = self._handle_absolute_location(deref(done.finalObject))
        return result

    def subscribe(self, str track_name, group_order=GroupOrder.Default):
        """Python-friendly subscribe method"""
        cdef SubscribeRequest req
        req.fullTrackName = self._make_track_name(track_name)
        req.groupOrder = group_order
        
        try:
            result = self._session.get().subscribe(req)
            return self._handle_track_handle(result)
        except Exception as e:
            error = self._handle_subscribe_error(e.error())
            raise SubscribeException(**error)