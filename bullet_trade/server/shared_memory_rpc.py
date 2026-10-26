"""Windows local RPC. Python 3.6 compatible; embedded verbatim in the QMT helper.

No sockets, disk-backed mappings, multiprocessing, or third-party dependencies.
One mailbox, with separate owner, transaction and short-lived state mutexes.
Never replay a published request. A claimed request cannot be cancelled safely.
"""
import hashlib
import json
import math
import struct
import sys
import time
from contextlib import contextmanager
from uuid import uuid4


IPC_DEFAULT_NAME = r"Local\BulletTrade_BigQMT_v1"
IPC_DEFAULT_SIZE = 16 * 1024 * 1024
IPC_HEADER = struct.Struct("<8sII16sQIddI32s")
IPC_MAGIC = b"BTQMTIPC"
IPC_VERSION = 1
IPC_IDLE, IPC_REQUEST, IPC_RUNNING, IPC_RESPONSE, IPC_OFFLINE = range(5)


class ShmError(RuntimeError):
    def __init__(self, code, message, uncertain=False):
        super().__init__(message)
        self.code = code
        self.uncertain = uncertain


def _ipc_ctypes():
    cached = sys.modules.get("ctypes")
    try:
        import ctypes
        return ctypes, "standard_import"
    except Exception:
        # Some embedded interpreters already loaded ctypes but reject imports.
        if cached is not None and all(hasattr(cached, n) for n in
                                      ("WinDLL", "c_void_p", "memmove", "string_at")):
            return cached, "loaded_modules"
        raise ShmError("IPC_UNAVAILABLE", "ctypes is unavailable in this QMT build")


class Win32Region:
    """Pagefile-backed named memory; handles/pointers have explicit Win64 types."""
    def __init__(self, name, size, create=False):
        if sys.platform != "win32":
            raise ShmError("IPC_UNAVAILABLE", "Shared-memory gateway requires Windows")
        if not name.startswith("Local\\") or "\\" in name[6:] or len(name) > 200:
            raise ValueError("IPC name must be Local\\<name> in the same Windows session")
        if not 4096 <= size <= 256 * 1024 * 1024:
            raise ValueError("IPC size must be between 4 KiB and 256 MiB")
        self.size = size
        self.ptr = None
        self.handles = []
        self.mutexes = {}
        self.owner = False
        self.ct, self.load_mode = _ipc_ctypes()
        c = self.ct
        self.k = c.WinDLL("kernel32", use_last_error=True)
        signatures = {
            "CreateFileMappingW": ([c.c_void_p, c.c_void_p, c.c_uint32, c.c_uint32, c.c_uint32, c.c_wchar_p], c.c_void_p),
            "OpenFileMappingW": ([c.c_uint32, c.c_int, c.c_wchar_p], c.c_void_p),
            "MapViewOfFile": ([c.c_void_p, c.c_uint32, c.c_uint32, c.c_uint32, c.c_size_t], c.c_void_p),
            "UnmapViewOfFile": ([c.c_void_p], c.c_int),
            "CreateMutexW": ([c.c_void_p, c.c_int, c.c_wchar_p], c.c_void_p),
            "WaitForSingleObject": ([c.c_void_p, c.c_uint32], c.c_uint32),
            "ReleaseMutex": ([c.c_void_p], c.c_int),
            "CloseHandle": ([c.c_void_p], c.c_int),
        }
        for func, (args, result) in signatures.items():
            fn = getattr(self.k, func)
            fn.argtypes, fn.restype = args, result
        try:
            for suffix in ("owner", "transaction", "state"):
                handle = self._checked(self.k.CreateMutexW(None, False, name + "." + suffix))
                self.handles.append(handle)
                self.mutexes[suffix] = handle
            if create:
                # Held by the QMT strategy thread for the complete serving lifetime.
                self.acquire("owner", 0, abandoned_ok=True)
                self.owner = True
                mapping = self._checked(self.k.CreateFileMappingW(c.c_void_p(-1), None, 4, 0, size, name))
            else:
                mapping = self._checked(self.k.OpenFileMappingW(6, False, name))
            self.handles.append(mapping)
            self.ptr = self._checked(self.k.MapViewOfFile(mapping, 6, 0, 0, size))
        except BaseException:
            self.close()
            raise

    def _checked(self, value):
        if not value:
            raise ShmError("IPC_UNAVAILABLE", "Win32 IPC failed (error=%s); check QMT, session and mapping size" % self.ct.get_last_error())
        return value

    def acquire(self, which, seconds, abandoned_ok=False):
        result = self.k.WaitForSingleObject(self.mutexes[which], min(0xfffffffe, max(0, int(seconds * 1000))))
        if result == 0x102:
            raise ShmError("IPC_BUSY", "Timed out acquiring IPC %s mutex" % which)
        if result == 0x80:
            if not abandoned_ok:
                self.k.ReleaseMutex(self.mutexes[which])
                raise ShmError("IPC_CORRUPT", "Abandoned IPC state mutex; restart both endpoints")
        elif result != 0:
            raise ShmError("IPC_UNAVAILABLE", "WaitForSingleObject failed: %s" % self.ct.get_last_error())

    def release(self, which):
        self._checked(self.k.ReleaseMutex(self.mutexes[which]))

    @contextmanager
    def locked(self, which="state", seconds=1.0, abandoned_ok=False):
        self.acquire(which, seconds, abandoned_ok)
        try:
            yield
        finally:
            self.release(which)

    def read(self, offset, length):
        self._bounds(offset, length)
        return self.ct.string_at(self.ptr + offset, length)

    def write(self, offset, data):
        self._bounds(offset, len(data))
        self.ct.memmove(self.ptr + offset, data, len(data))

    def _bounds(self, offset, length):
        if self.ptr is None or offset < 0 or length < 0 or offset + length > self.size:
            raise ShmError("IPC_CORRUPT", "IPC memory access outside configured mapping")

    def close(self):
        if self.ptr is not None:
            self.k.UnmapViewOfFile(self.ptr)
            self.ptr = None
        if self.owner:
            self.k.ReleaseMutex(self.mutexes["owner"])
            self.owner = False
        for handle in reversed(self.handles):
            self.k.CloseHandle(handle)
        self.handles = []


def _ipc_header(region):
    h = list(IPC_HEADER.unpack(region.read(0, IPC_HEADER.size)))
    if h[:3] != [IPC_MAGIC, IPC_VERSION, region.size] or h[5] not in range(5):
        raise ShmError("IPC_PROTOCOL", "IPC header/version/size mismatch; check both endpoints")
    if not math.isfinite(h[6]) or not math.isfinite(h[7]):
        raise ShmError("IPC_CORRUPT", "Invalid IPC timestamp")
    return h


def _ipc_encode(value, region):
    raw = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(raw) > region.size - IPC_HEADER.size:
        raise ShmError("IPC_TOO_LARGE", "JSON exceeds IPC capacity; split query or increase both mapping sizes")
    return raw


def _ipc_read(region, header):
    size = header[8]
    if size <= 0 or size > region.size - IPC_HEADER.size:
        raise ShmError("IPC_CORRUPT", "Invalid IPC payload length")
    raw = region.read(IPC_HEADER.size, size)
    if hashlib.sha256(raw).digest() != header[9]:
        raise ShmError("IPC_CORRUPT", "IPC payload checksum mismatch")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeError):
        raise ShmError("IPC_CORRUPT", "Invalid IPC JSON")
    if not isinstance(value, dict):
        raise ShmError("IPC_CORRUPT", "IPC envelope must be an object")
    return value


def _ipc_write(region, header, state, raw=None):
    if raw is not None:
        region.write(IPC_HEADER.size, raw)
        header[8], header[9] = len(raw), hashlib.sha256(raw).digest()
    header[5] = state
    region.write(0, IPC_HEADER.pack(*header))


class ShmRpcServer:
    def __init__(self, region, dispatch):
        self.region, self.dispatch = region, dispatch
        self.session = uuid4().bytes
        with region.locked():
            header = [IPC_MAGIC, IPC_VERSION, region.size, self.session, 0,
                      IPC_IDLE, 0.0, time.time(), 0, b"\0" * 32]
            region.write(0, IPC_HEADER.pack(*header))

    def poll(self):
        region = self.region
        with region.locked():
            h = _ipc_header(region)
            if h[3] != self.session:
                raise ShmError("IPC_RESTARTED", "IPC owner session changed")
            h[7] = time.time()
            if h[5] != IPC_REQUEST:
                region.write(0, IPC_HEADER.pack(*h))
                return False
            request = _ipc_read(region, h)
            expired = time.time() >= h[6]
            _ipc_write(region, h, IPC_RUNNING)
        # QMT calls happen only on the serving (init) thread, outside state lock.
        if expired:
            response = {"ipc_error": "IPC_EXPIRED", "message": "Request expired before dispatch", "uncertain": False}
        else:
            try:
                response = self.dispatch(request)
            except Exception as exc:
                response = {"ipc_error": "IPC_DISPATCH_FAILED", "message": "%s: %s" % (type(exc).__name__, exc), "uncertain": True}
        try:
            raw = _ipc_encode(response, region)
        except (ShmError, ValueError, TypeError) as exc:
            raw = _ipc_encode({"ipc_error": "IPC_RESPONSE_INVALID", "message": str(exc), "uncertain": True}, region)
        with region.locked():
            current = _ipc_header(region)
            if current[3] != self.session or current[4] != h[4] or current[5] != IPC_RUNNING:
                raise ShmError("IPC_CORRUPT", "In-flight IPC request was overwritten")
            h[7] = time.time()
            _ipc_write(region, h, IPC_RESPONSE, raw)
        return True

    def close(self):
        try:
            with self.region.locked():
                h = _ipc_header(self.region)
                if h[3] == self.session:
                    _ipc_write(self.region, h, IPC_OFFLINE)
        finally:
            self.region.close()


class ShmRpcClient:
    def __init__(self, name=IPC_DEFAULT_NAME, size=IPC_DEFAULT_SIZE, poll_seconds=0.01,
                 region_factory=Win32Region):
        self.name, self.size = name, size
        self.poll_seconds = max(0.001, float(poll_seconds))
        self.region_factory = region_factory

    def request(self, request, timeout=10.0, prepare=None):
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("IPC timeout must be finite and positive")
        deadline = time.monotonic() + timeout
        region = self.region_factory(self.name, self.size, create=False)
        published = False
        try:
            with region.locked("transaction", max(0, deadline - time.monotonic()), abandoned_ok=True):
                # Never overwrite a request still being executed, even after timeout.
                while True:
                    with region.locked():
                        h = _ipc_header(region)
                        if h[5] == IPC_OFFLINE or time.time() - h[7] > max(timeout, 5):
                            raise ShmError("IPC_UNAVAILABLE", "QMT helper is offline or unresponsive")
                        if h[5] not in (IPC_REQUEST, IPC_RUNNING):
                            break
                    if time.monotonic() >= deadline:
                        raise ShmError("IPC_BUSY", "Previous QMT request is still in flight; nothing submitted")
                    time.sleep(self.poll_seconds)
                session, seq = h[3], h[4] + 1
                envelope = dict(request)
                if prepare is not None:
                    envelope.update(prepare(session.hex()) or {})
                raw = _ipc_encode(envelope, region)
                if time.monotonic() >= deadline:
                    raise ShmError("IPC_EXPIRED", "IPC deadline elapsed before publication")
                with region.locked():
                    current = _ipc_header(region)
                    if current[3] != session or current[5] not in (IPC_IDLE, IPC_RESPONSE):
                        raise ShmError("IPC_RESTARTED", "QMT changed before publication; nothing submitted")
                    current[4], current[6] = seq, time.time() + max(0, deadline - time.monotonic())
                    # From this point, partial writes/failures are also uncertain.
                    published = True
                    _ipc_write(region, current, IPC_REQUEST, raw)
                while True:
                    with region.locked():
                        current = _ipc_header(region)
                        if current[3] != session or current[4] != seq or current[5] == IPC_OFFLINE:
                            raise ShmError("IPC_RESTARTED", "QMT restarted during request; reconcile orders before retry", True)
                        if current[5] == IPC_RESPONSE:
                            response = _ipc_read(region, current)
                            _ipc_write(region, current, IPC_IDLE)
                            if response.get("ipc_error"):
                                published = bool(response.get("uncertain", True))
                                raise ShmError(response["ipc_error"], response.get("message", "IPC error"), published)
                            return response
                        if time.monotonic() >= deadline:
                            if current[5] == IPC_REQUEST:
                                _ipc_write(region, current, IPC_IDLE)
                                published = False
                                raise ShmError("IPC_EXPIRED", "Request withdrawn before QMT claimed it")
                            raise ShmError("IPC_OUTCOME_UNKNOWN", "QMT claimed request; outcome unknown, do not resubmit", True)
                    time.sleep(self.poll_seconds)
        except ShmError as exc:
            exc.uncertain = exc.uncertain or published
            raise
        finally:
            region.close()
