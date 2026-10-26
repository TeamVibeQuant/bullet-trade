import builtins
import importlib.util
import multiprocessing
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import pytest

from bullet_trade.server import shared_memory_rpc as ipc
from bullet_trade.server.big_qmt_shm import BigQmtShmTransport, ShmJournal
from bullet_trade.server.adapters.big_qmt import BigQmtGatewayClient, BigQmtGatewayConfig


class MemoryRegion:
    """Test storage only; production always uses Win32Region."""
    def __init__(self, size=16384):
        self.size = size
        self.data = bytearray(size)
        self.locks = {key: threading.Lock() for key in ("transaction", "state")}

    @contextmanager
    def locked(self, which="state", seconds=1, abandoned_ok=False):
        if not self.locks[which].acquire(timeout=seconds):
            raise ipc.ShmError("IPC_BUSY", "lock timeout")
        try:
            yield
        finally:
            self.locks[which].release()

    def read(self, offset, length):
        return bytes(self.data[offset:offset + length])

    def write(self, offset, data):
        assert offset + len(data) <= self.size
        self.data[offset:offset + len(data)] = data

    def close(self):
        pass


@contextmanager
def serving(dispatch, region=None):
    region = region or MemoryRegion()
    server = ipc.ShmRpcServer(region, dispatch)
    stopped = threading.Event()
    errors = []

    def run():
        try:
            while not stopped.is_set():
                server.poll()
                stopped.wait(0.001)
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    client = ipc.ShmRpcClient(size=region.size, region_factory=lambda *a, **k: region)
    try:
        yield client, server
    finally:
        stopped.set()
        thread.join(3)
        assert not thread.is_alive()
        assert not errors


def load_helper():
    path = Path(__file__).resolve().parents[2] / "helpers/big_qmt_gateway_strategy_sample.py"
    spec = importlib.util.spec_from_file_location("isolated_qmt_helper", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_roundtrip_unicode_and_concurrent_requests():
    calls = []
    def dispatch(req):
        calls.append(req["n"])
        return {"response": {"n": req["n"], "name": "测试股票"}}
    with serving(dispatch) as (client, server):
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda n: client.request({"n": n}), range(40)))
    assert [r["response"]["n"] for r in results] == list(range(40))
    assert len(set(calls)) == 40
    assert results[0]["response"]["name"] == "测试股票"


def test_unclaimed_timeout_withdraws_and_never_executes():
    region = MemoryRegion()
    calls = []
    server = ipc.ShmRpcServer(region, lambda req: calls.append(req))
    client = ipc.ShmRpcClient(region_factory=lambda *a, **k: region)
    with pytest.raises(ipc.ShmError) as err:
        client.request({"order": 1}, timeout=0.02)
    assert err.value.code == "IPC_EXPIRED" and not err.value.uncertain
    assert server.poll() is False
    assert not calls


def test_claimed_timeout_is_unknown_and_late_response_not_misattributed():
    called = threading.Event()
    release = threading.Event()
    calls = []
    def dispatch(req):
        calls.append(req)
        called.set()
        release.wait(2)
        return {"response": req}
    with serving(dispatch) as (client, server):
        with pytest.raises(ipc.ShmError) as err:
            client.request({"order": 1}, timeout=0.03)
        assert called.is_set()
        assert err.value.uncertain
        with pytest.raises(ipc.ShmError) as busy:
            client.request({"order": 2}, timeout=0.02)
        assert busy.value.code == "IPC_BUSY" and not busy.value.uncertain
        release.set()
        assert client.request({"query": 3}) == {"response": {"query": 3}}
    assert calls == [{"order": 1}, {"query": 3}]


def test_session_restart_never_replays_old_request():
    region = MemoryRegion()
    ipc.ShmRpcServer(region, lambda req: {})
    client = ipc.ShmRpcClient(region_factory=lambda *a, **k: region)
    with ThreadPoolExecutor() as pool:
        future = pool.submit(client.request, {"order": 1}, 1)
        deadline = time.monotonic() + 1
        while ipc._ipc_header(region)[5] != ipc.IPC_REQUEST and time.monotonic() < deadline:
            time.sleep(0.001)
        replacement = ipc.ShmRpcServer(region, lambda req: pytest.fail("old request replayed"))
        with pytest.raises(ipc.ShmError) as err:
            future.result()
        assert err.value.code == "IPC_RESTARTED" and err.value.uncertain
        assert replacement.poll() is False


def test_expired_request_and_corruption_do_not_dispatch():
    region = MemoryRegion()
    server = ipc.ShmRpcServer(region, lambda req: pytest.fail("must not dispatch"))
    header = ipc._ipc_header(region)
    header[6] = time.time() - 1
    ipc._ipc_write(region, header, ipc.IPC_REQUEST, ipc._ipc_encode({"order": 1}, region))
    assert server.poll()
    response = ipc._ipc_read(region, ipc._ipc_header(region))
    assert response["ipc_error"] == "IPC_EXPIRED"
    header = ipc._ipc_header(region)
    header[6] = time.time() + 10
    ipc._ipc_write(region, header, ipc.IPC_REQUEST, b'{"order":1}')
    region.data[ipc.IPC_HEADER.size] ^= 1
    with pytest.raises(ipc.ShmError, match="checksum"):
        server.poll()


def test_size_and_version_fail_before_dispatch():
    region = MemoryRegion()
    ipc.ShmRpcServer(region, lambda req: {})
    client = ipc.ShmRpcClient(region_factory=lambda *a, **k: region)
    with pytest.raises(ipc.ShmError) as err:
        client.request({"large": "a" * region.size})
    assert err.value.code == "IPC_TOO_LARGE" and not err.value.uncertain
    region.data[8] = 2
    with pytest.raises(ipc.ShmError) as err:
        client.request({})
    assert err.value.code == "IPC_PROTOCOL"


def test_oversize_response_is_unknown_and_channel_can_recover():
    with serving(lambda req: {"large": "a" * 20000} if req else {"ok": True}) as (client, _):
        with pytest.raises(ipc.ShmError) as err:
            client.request({"order": 1})
        assert err.value.code == "IPC_RESPONSE_INVALID" and err.value.uncertain
        assert client.request({}) == {"ok": True}


def test_helper_loads_and_dispatches_without_file_or_network_io(monkeypatch):
    # Read/compile source first; simulate the QMT editor executing it with IO blocked.
    path = Path(__file__).resolve().parents[2] / "helpers/big_qmt_gateway_strategy_sample.py"
    code = compile(path.read_bytes(), str(path), "exec")
    real_import = builtins.__import__
    attempts = []
    def limited_import(name, *args, **kwargs):
        if name.split(".")[0] in {"socket", "tornado", "urllib", "sqlite3", "multiprocessing", "mmap"}:
            attempts.append(name)
            raise AssertionError("forbidden network/IO import: " + name)
        return real_import(name, *args, **kwargs)
    def no_file(*args, **kwargs):
        raise AssertionError("filesystem access")
    monkeypatch.setattr(builtins, "__import__", limited_import)
    monkeypatch.setattr(builtins, "open", no_file)
    import os
    monkeypatch.setattr(os, "getcwd", no_file)
    ns = {"__name__": "restricted_helper"}
    exec(code, ns)
    ns["RUNTIME"].context_info = object()
    ns["passorder"] = lambda *args: "order-test-1"
    response = ns["_shm_dispatch"]({"method": "POST", "path": "/place_order",
        "headers": {"X-BulletTrade-Password": ns["GATEWAY_PASSWORD"], "X-BulletTrade-Secret": ns["GATEWAY_SECRET"]},
        "payload": {"security": "000001.XSHE", "amount": 100, "price": 10, "request_id": "r", "account_id": "test"}})
    assert response["response"]["ok"]
    assert response["order_tags"]["by_id"]["order-test-1"]["request_id"] == "r"
    assert not attempts


def test_ctypes_cached_import_fallback(monkeypatch):
    from types import SimpleNamespace
    cached = SimpleNamespace(WinDLL=True, c_void_p=True, memmove=True, string_at=True)
    monkeypatch.setitem(sys.modules, "ctypes", cached)
    real_import = builtins.__import__
    def blocked(name, *args, **kwargs):
        if name == "ctypes":
            raise ImportError("disabled")
        return real_import(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", blocked)
    assert ipc._ipc_ctypes() == (cached, "loaded_modules")


def test_external_adapter_helper_roundtrip_and_tag_restore(tmp_path):
    helper = load_helper()
    helper.ACCOUNT_ID = "test"
    helper.RUNTIME.context_info = type("Context", (), {"get_full_tick": lambda self, *a, **kw: {"000001.SZ": {"lastPrice": 10}}})()
    calls = []
    helper.passorder = lambda *args: calls.append(args) or "o-1"
    config = BigQmtGatewayConfig(password=helper.GATEWAY_PASSWORD, secret=helper.GATEWAY_SECRET,
                                 shm_journal=str(tmp_path / "state.sqlite3"))
    with serving(helper._shm_dispatch) as (rpc, _):
        client = BigQmtGatewayClient(config)
        client._shm.rpc = rpc
        assert client.request_json("/health", None, "GET")["transport"] == "shm"
        payload = {"security": "000001.XSHE", "amount": 100, "price": 10, "idempotency_key": "one", "sub_account_id": "sub-a"}
        order = client.request_json("/place_order", payload, "POST")
        assert order["order_id"] == "o-1"
        # Restart external transport: replay returns the durable result, no new passorder.
        client = BigQmtGatewayClient(config)
        client._shm.rpc = rpc
        assert client.request_json("/place_order", payload, "POST") == order
        assert len(calls) == 1
        # Simulate a fresh helper and restore virtual account tags via authenticated IPC.
        helper.ORDER_TAG_STORE_LOADED = False
        helper.ORDER_TAGS_BY_ID = {}
        helper.PENDING_ORDER_TAGS = []
        client.request_json("/data/snapshot", {"security": "000001.XSHE"}, "POST")
        assert helper.ORDER_TAGS_BY_ID["o-1"]["sub_account_id"] == "sub-a"


def test_uncertain_intent_survives_restart_and_blocks_new_trades(tmp_path):
    cfg = BigQmtGatewayConfig(shm_journal=str(tmp_path / "journal.sqlite3"))
    transport = BigQmtShmTransport(cfg)
    class Uncertain:
        def request(self, *a, **kw):
            raise ipc.ShmError("IPC_OUTCOME_UNKNOWN", "lost after claim", True)
    transport.rpc = Uncertain()
    with pytest.raises(ipc.ShmError, match="outcome unknown"):
        transport.request("/place_order", {"request_id": "lost-1"}, "POST")
    transport = BigQmtShmTransport(cfg)
    with pytest.raises(ipc.ShmError, match="lost-1"):
        transport.request("/place_order", {"request_id": "different"}, "POST")
    # Read queries remain available for reconciliation.
    class Query:
        def request(self, *a, **kw):
            return {"response": {"ok": True, "value": []}}
    transport.rpc = Query()
    assert transport.request("/orders", {}, "POST")["ok"]


def test_journal_conflicting_payload_rejected(tmp_path):
    journal = ShmJournal(tmp_path / "journal.db", "channel")
    assert journal.begin("k", "fingerprint-a", "r") is None
    journal.finish("k", {"ok": True})
    assert journal.begin("k", "fingerprint-a", "r2") == {"ok": True}
    with pytest.raises(ipc.ShmError) as err:
        journal.begin("k", "fingerprint-b", "r2")
    assert err.value.code == "IPC_IDEMPOTENCY_CONFLICT"


def test_local_queue_timeout_does_not_create_or_send_intent(tmp_path):
    cfg = BigQmtGatewayConfig(timeout_seconds=0.02, shm_journal=str(tmp_path / "journal.db"))
    transport = BigQmtShmTransport(cfg)
    transport.lock.acquire()
    try:
        with pytest.raises(ipc.ShmError) as err:
            transport.request("/place_order", {}, "POST")
        assert err.value.code == "IPC_BUSY" and not err.value.uncertain
        assert not Path(cfg.shm_journal).exists()
    finally:
        transport.lock.release()


def test_bad_auth_cannot_restore_tags_or_call_qmt():
    helper = load_helper()
    helper.passorder = lambda *args: pytest.fail("unauthorized call")
    response = helper._shm_dispatch({"path": "/place_order", "method": "POST", "payload": {},
                                    "headers": {}, "order_tags": {"by_id": {"foreign": {}}, "pending": []}})
    assert response["response"]["code"] == "AUTH_FAILED"
    assert not helper.ORDER_TAG_STORE_LOADED and not helper.ORDER_TAGS_BY_ID


def test_explicit_http_transport_retains_legacy_client(monkeypatch):
    import io
    import json
    import urllib.request
    client = BigQmtGatewayClient(BigQmtGatewayConfig(transport="http", password="test"))
    sent = []
    def urlopen(request, timeout):
        sent.append((request.full_url, json.loads(request.data), timeout))
        return io.BytesIO(b'{"ok":true,"value":{"available_cash":123}}')
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    assert client.request_json("/account", {"account_id": "test"}, "POST") == {"available_cash": 123}
    assert sent == [("http://127.0.0.1:9000/account", {"account_id": "test"}, 10)]
    assert client.qmt_status()["gateway_transport"] == "http"


def test_helper_serves_and_releases_on_init_thread(monkeypatch):
    helper = load_helper()
    region = MemoryRegion()
    region.load_mode = "test"
    thread_ids = []
    closed = []
    def dispatch(req):
        thread_ids.append(threading.get_ident())
        helper.stop(None)
        return {"response": {"ok": True}}
    original_poll = helper.ShmRpcServer.poll
    def poll_once(server):
        h = ipc._ipc_header(region)
        h[6] = time.time() + 1
        ipc._ipc_write(region, h, ipc.IPC_REQUEST, b'{}')
        return original_poll(server)
    monkeypatch.setattr(helper, "Win32Region", lambda *args, **kwargs: region)
    monkeypatch.setattr(helper.ShmRpcServer, "poll", poll_once)
    monkeypatch.setattr(helper, "_shm_dispatch", dispatch)
    region.close = lambda: closed.append(threading.get_ident())
    helper._start_shm_server()
    assert thread_ids == closed == [threading.get_ident()]
    assert helper.RUNTIME.shm_server is None
    assert ipc._ipc_header(region)[5] == ipc.IPC_OFFLINE


def test_embedded_source_is_current_and_python36_compatible():
    import ast
    root = Path(__file__).resolve().parents[2]
    helper = (root / "helpers/big_qmt_gateway_strategy_sample.py").read_text(encoding="gbk")
    source = (root / "bullet_trade/server/shared_memory_rpc.py").read_text().rstrip()
    embedded = helper.split("# BEGIN GENERATED SHARED MEMORY RPC\n")[1].split("# END GENERATED SHARED MEMORY RPC")[0].rstrip()
    assert embedded == source
    ast.parse(helper, feature_version=(3, 6))


def _windows_server(name, ready, stop):
    region = ipc.Win32Region(name, 65536, create=True)
    server = ipc.ShmRpcServer(region, lambda req: {"response": req})
    ready.set()
    try:
        while not stop.is_set():
            server.poll()
            time.sleep(0.001)
    finally:
        server.close()


@pytest.mark.skipif(sys.platform != "win32", reason="real Win32 cross-process mapping")
def test_windows_native_process_roundtrip_and_single_owner():
    name = "Local\\BT_IPC_TEST_" + uuid4().hex
    ctx = multiprocessing.get_context("spawn")
    ready, stop = ctx.Event(), ctx.Event()
    process = ctx.Process(target=_windows_server, args=(name, ready, stop))
    process.start()
    try:
        assert ready.wait(15)
        client = ipc.ShmRpcClient(name, 65536)
        assert client.request({"unicode": "测试", "value": "a" * 32000})["response"]["unicode"] == "测试"
        with pytest.raises(ipc.ShmError) as err:
            ipc.Win32Region(name, 65536, create=True)
        assert err.value.code == "IPC_BUSY"
    finally:
        stop.set()
        process.join(10)
        if process.is_alive():
            process.terminate()
            process.join()
    assert process.exitcode == 0
