# moxygen/common/common.pyx
# distutils: language = c++
# cython: language_level = 3

from libcpp.memory cimport shared_ptr
from libfolly.eventbase cimport EventBase
import threading

# Only declare the enums/types we actively need to reference in Python
cdef extern from "moxygen/MoQFramer.h" namespace "moxygen":
    cpdef enum class GroupOrder:
        Default
        OldestFirst
        NewestFirst

    cpdef enum class LocationType:
        LatestGroup
        LatestObject
        AbsoluteStart
        AbsoluteRange

cdef extern from "moxygen/MoQFramer.h" namespace "moxygen":
    # Simple enums are fine to declare directly
    cpdef enum class SubscribeErrorCode(uint32_t):
        INTERNAL_ERROR = 0
        INVALID_RANGE = 1
        RETRY_TRACK_ALIAS = 2
        TRACK_NOT_EXIST = 3
        UNAUTHORIZED = 4
        TIMEOUT = 5

    # Let's declare exactly what's in the C++ header
    cdef struct AbsoluteLocation:
        uint64_t group
        uint64_t object

    cdef cppclass Optional[T]:
        pass

    cdef struct SubscribeError:
        uint64_t subscribeID
        uint64_t errorCode
        string reasonPhrase
        Optional[uint64_t] retryAlias  # Declare the full type

    cdef struct SubscribeDone:
        uint64_t subscribeID
        SubscribeDoneStatusCode statusCode
        string reasonPhrase
        Optional[AbsoluteLocation] finalObject  # Declare the full type

# Basic initialization and event loop functionality
cdef extern from "folly/init/Init.h" namespace "folly":
    cdef cppclass Init:
        Init(int* argc, char*** argv, bool removeFlags) except +

cdef extern from "glog/logging.h" namespace "google":
    void InitGoogleLogging(const char* name)
    void InstallFailureSignalHandler()
    void SetMinLogLevel(int level)

_initialized = False

def init_logging():
    """Initialize folly and logging infrastructure"""
    global _initialized
    if not _initialized:
        cdef int argc = 1
        cdef char* argv[1]
        argv[0] = "moxygen"
        cdef char** argv_ptr = argv
        
        Init(&argc, &argv_ptr, True)
        InitGoogleLogging(argv[0])
        InstallFailureSignalHandler()
        _initialized = True

T = TypeVar('T')

cdef class EventLoopThread:
    cdef shared_ptr[EventBase] _evb
    cdef object _thread
    cdef bint _running
    cdef object _lock

    def __cinit__(self, shared_ptr[EventBase] evb):
        self._evb = evb
        self._thread = None
        self._running = False
        self._lock = threading.Lock()

    def start(self):
        with self._lock:
            if not self._running:
                self._running = True
                self._thread = threading.Thread(target=self._run_loop, daemon=True)
                self._thread.start()

    def stop(self, timeout=1.0):
        with self._lock:
            if self._running:
                self._running = False
                if self._thread and self._thread.is_alive():
                    self._thread.join(timeout=timeout)

    def _run_loop(self):
        while self._running:
            self._evb.get().loopOnce()

class SessionPair(Generic[T]):
    def __init__(self, client: T, server: T):
        self.client = client
        self.server = server
        self._event_loop = None

    def start(self):
        self.client.start()
        self.server.start()
        self._event_loop = EventLoopThread(self.client._evb)
        self._event_loop.start()

    def stop(self):
        if self._event_loop:
            self._event_loop.stop()
        self.client.close()
        self.server.close()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

def run_with_timeout(func, timeout: float = 1.0, *args, **kwargs):
    result = []
    error = []
    
    def _run():
        try:
            result.append(func(*args, **kwargs))
        except Exception as e:
            error.append(e)
    
    thread = threading.Thread(target=_run)
    thread.daemon = True
    thread.start()
    thread.join(timeout=timeout)
    
    if error:
        raise error[0]
    if thread.is_alive():
        raise TimeoutError(f"Operation timed out after {timeout} seconds")
    return result[0]
