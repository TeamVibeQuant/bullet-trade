"""Offline regressions for account queries, initialization and final persistence."""

import asyncio
import copy
import json
import pickle
import sys
import threading
from types import ModuleType, SimpleNamespace

import pytest

from bullet_trade.broker.qmt_remote import RemoteQmtBroker
from bullet_trade.broker.simulator import SimulatorBroker
from bullet_trade.core import live_engine as engine_module, live_runtime
from bullet_trade.core.async_scheduler import AsyncScheduler
from bullet_trade.core.event_bus import EventBus
from bullet_trade.core.events import SystemStopEvent
from bullet_trade.core.globals import g
from bullet_trade.core.live_engine import LiveEngine
from bullet_trade.core.models import SubPortfolioConfig
from bullet_trade.core.scheduler import unschedule_all
from bullet_trade.core.settings import set_subportfolios


class RecordingBroker(SimulatorBroker):
    def __init__(self):
        super().__init__(account_id="offline-lifecycle")
        self.events = []
        self.query_error = None
        self.snapshot = {
            "available_cash": 900.0,
            "total_value": 2000.0,
            "positions": [{
                "security": "000001.XSHE", "name": "offline",
                "amount": 100, "closeable_amount": 100,
                "avg_cost": 10.0, "current_price": 11.0,
                "market_value": 1100.0,
            }],
        }

    def connect(self):
        self.events.append("connect")
        self._connected = True
        return True

    def sync_account(self):
        self.events.append("query")
        if not self._connected:
            raise RuntimeError("remote connection 尚未启动")
        if self.query_error:
            raise self.query_error
        return copy.deepcopy(self.snapshot)

    def cleanup(self):
        self.events.append("disconnect")
        self._connected = False


@pytest.fixture
def lifecycle(tmp_path, monkeypatch):
    live_runtime.close_live_runtime(save=False)
    unschedule_all()
    monkeypatch.setenv("BULLET_TRADE_HOME", str(tmp_path))
    strategy_path = tmp_path / "strategy.py"
    strategy_path.write_text("# offline strategy\n", encoding="utf-8")
    strategy = ModuleType("strategy")
    monkeypatch.setitem(sys.modules, "strategy", strategy)
    loader = SimpleNamespace(
        load_strategy=lambda: None,
        initialize_func=None, handle_data_func=None,
        before_trading_start_func=None, after_trading_end_func=None,
    )
    monkeypatch.setattr(engine_module, "BacktestEngine", lambda **kwargs: loader)
    broker = RecordingBroker()
    engine = LiveEngine(
        strategy_path, broker_factory=lambda: broker,
        live_config={
            "runtime_dir": str(tmp_path / "runtime"),
            "g_autosave_enabled": False, "g_autosave_interval": 3600,
            "account_sync_enabled": False, "order_sync_enabled": False,
            "tick_sync_enabled": False, "risk_check_enabled": False,
            "broker_heartbeat_interval": 0, "portfolio_refresh_throttle_ms": 0,
        },
    )
    # No market-data provider or trading loop participates in these tests.
    monkeypatch.setattr(engine, "_start_background_jobs", lambda: None)
    monkeypatch.setattr(engine, "_fetch_tick_snapshot", lambda security: None)

    def initialize(context):
        set_subportfolios([
            SubPortfolioConfig(0.0, "stock"),
            SubPortfolioConfig(context.portfolio.starting_cash, "stock"),
        ])

    loader.initialize_func = initialize
    strategy.process_initialize = initialize
    yield engine, broker, loader, strategy
    live_runtime.close_live_runtime(save=False)
    engine._release_live_locks()
    unschedule_all()


async def bootstrap(engine):
    engine._loop = asyncio.get_running_loop()
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(engine._loop)
    engine.async_scheduler = AsyncScheduler()
    await engine._bootstrap()


@pytest.mark.asyncio
@pytest.mark.parametrize("restored", [False, True])
async def test_initializers_use_broker_nav_without_querying_partial_subportfolios(
    lifecycle, monkeypatch, restored,
):
    engine, broker, loader, strategy = lifecycle
    observations = []

    def initialize(context):
        observations.append(context.portfolio.starting_cash)
        set_subportfolios([
            SubPortfolioConfig(0.0, "stock"),
            SubPortfolioConfig(context.portfolio.starting_cash, "stock"),
        ])
        # Repeated property reads must not reconcile the temporary all-cash setup.
        assert context.portfolio.subportfolios[1].available_cash == 2000.0
        assert context.portfolio.starting_cash == 2000.0
        assert broker.events == ["connect", "query"]
        assert live_runtime._autosave_thread is None

    loader.initialize_func = initialize
    strategy.process_initialize = initialize
    if restored:
        directory = engine.strategy_path.parent / "runtime"
        directory.mkdir()
        (directory / "subportfolios.json").write_text(json.dumps({
            "saved_at": "previous",
            "subportfolios": {
                "0": {"available_cash": 0.0, "total_value": 0.0, "positions": {}},
                "1": {
                    "available_cash": 900.0, "total_value": 1900.0,
                    "positions": {"000001.XSHE": {
                        "security": "000001.XSHE", "total_amount": 100,
                        "avg_cost": 10.0, "price": 10.0, "value": 1000.0,
                    }},
                },
            },
        }), encoding="utf-8")
        monkeypatch.setattr(engine_module, "runtime_restored", lambda: True)
        monkeypatch.setattr(engine_module, "load_strategy_metadata", lambda: {
            "version": 1, "strategy_hash": "previous-version",
        })
        strategy.after_code_changed = initialize

    await bootstrap(engine)
    assert observations == [2000.0, 2000.0]
    saved = live_runtime.load_subportfolios()["subportfolios"]["1"]
    assert saved["available_cash"] == 900.0
    assert saved["positions"]["000001.XSHE"]["total_amount"] == 100
    assert saved["total_value"] == 2000.0
    before = broker.events.count("query")
    assert engine.context.portfolio.available_cash == 900.0
    assert broker.events.count("query") > before
    await engine._shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("autosave", [False, True])
async def test_shutdown_saves_once_before_disconnect_and_detaches_late_callbacks(
    lifecycle, monkeypatch, autosave,
):
    engine, broker, _, _ = lifecycle
    engine.config.g_autosave_enabled = autosave
    callbacks = set()
    monkeypatch.setattr(live_runtime.atexit, "register", callbacks.add)
    monkeypatch.setattr(live_runtime.atexit, "unregister", callbacks.discard)
    await bootstrap(engine)
    worker = live_runtime._autosave_thread
    broker.snapshot["available_cash"] = 950.0
    broker.snapshot["total_value"] = 2050.0
    broker.events.clear()
    original_cleanup = broker.cleanup

    def cleanup():
        saved = json.loads((engine.strategy_path.parent / "runtime" / "subportfolios.json").read_text())
        assert saved["subportfolios"]["1"]["available_cash"] == 950.0
        assert engine._runtime_lock is not None
        assert worker is None or not worker.is_alive()
        original_cleanup()

    monkeypatch.setattr(broker, "cleanup", cleanup)
    await engine._shutdown()
    path = engine.strategy_path.parent / "runtime" / "subportfolios.json"
    final_bytes = path.read_bytes()
    assert broker.events == ["query", "disconnect"]
    assert not callbacks
    assert live_runtime._portfolio_ref is None
    assert live_runtime._portfolio_price_refresher is None
    assert engine._runtime_lock is None
    # Simulate both the old run.finally and a delayed caller after shutdown.
    live_runtime.save_g()
    engine.refresh_account_snapshot(force=True)
    await engine._shutdown()
    assert path.read_bytes() == final_bytes
    assert broker.events == ["query", "disconnect"]


@pytest.mark.parametrize("exit_error,exit_code", [(None, 0), (KeyboardInterrupt, 0), (RuntimeError, 2)])
def test_run_finalizes_once_for_normal_interrupt_and_error(lifecycle, monkeypatch, exit_error, exit_code):
    engine, broker, _, _ = lifecycle

    async def run_loop():
        broker.events.clear()
        broker.snapshot["available_cash"] = 950.0
        broker.snapshot["total_value"] = 2050.0
        if exit_error:
            raise exit_error("offline stop")

    monkeypatch.setattr(engine, "_run_loop", run_loop)
    assert engine.run() == exit_code
    assert broker.events == ["query", "disconnect"]
    saved = json.loads((engine.strategy_path.parent / "runtime" / "subportfolios.json").read_text())
    assert saved["subportfolios"]["1"]["available_cash"] == 950.0


@pytest.mark.parametrize("failure", ["initialize", "account"])
def test_bootstrap_failure_preserves_previous_runtime(lifecycle, failure):
    engine, broker, loader, _ = lifecycle
    directory = engine.strategy_path.parent / "runtime"
    directory.mkdir()
    g_bytes = pickle.dumps({"last_good": 123})
    snapshot_bytes = b'{"saved_at": "previous", "subportfolios": {}}'
    (directory / "g.pkl").write_bytes(g_bytes)
    (directory / "subportfolios.json").write_bytes(snapshot_bytes)

    def initialize(context):
        g.last_good = -1
        raise RuntimeError("incomplete initialization")

    loader.initialize_func = initialize
    if failure == "account":
        broker.query_error = TimeoutError("offline timeout")
    assert engine.run() == 2
    assert (directory / "g.pkl").read_bytes() == g_bytes
    assert (directory / "subportfolios.json").read_bytes() == snapshot_bytes
    assert live_runtime._autosave_thread is None
    assert broker.events[-1] == "disconnect"
    assert engine._runtime_lock is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["disconnected", "timeout", "empty"])
async def test_failed_final_query_keeps_last_snapshot_without_second_query(lifecycle, failure):
    engine, broker, _, _ = lifecycle
    await bootstrap(engine)
    path = engine.strategy_path.parent / "runtime" / "subportfolios.json"
    previous = path.read_bytes()
    broker.events.clear()
    if failure == "disconnected":
        broker._connected = False
    elif failure == "timeout":
        broker.query_error = TimeoutError("offline timeout")
    else:
        broker.snapshot = {}
    await engine._shutdown()
    assert path.read_bytes() == previous
    assert broker.events.count("query") == (0 if failure == "disconnected" else 1)
    assert broker.events[-1] == "disconnect"


def test_portfolio_access_before_connect_does_not_query(lifecycle):
    engine, broker, _, _ = lifecycle
    engine._ensure_broker_created()
    _ = engine.context.portfolio.available_cash
    engine.refresh_account_snapshot(force=True)
    assert broker.events == []


@pytest.mark.asyncio
async def test_stop_event_failure_still_finalizes_runtime(lifecycle, monkeypatch):
    engine, broker, _, _ = lifecycle

    async def run_loop():
        broker.events.clear()

    async def emit(event):
        if isinstance(event, SystemStopEvent):
            raise RuntimeError("stop hook failed")

    monkeypatch.setattr(engine, "_run_loop", run_loop)
    monkeypatch.setattr(engine_module, "EventBus", lambda loop: SimpleNamespace(emit=emit))
    with pytest.raises(RuntimeError, match="stop hook failed"):
        await engine.start()
    assert broker.events == ["query", "disconnect"]
    assert engine._runtime_lock is None


@pytest.mark.asyncio
@pytest.mark.parametrize("already_cancelled", [False, True])
async def test_shutdown_waits_for_inflight_background_query(lifecycle, monkeypatch, already_cancelled):
    engine, broker, _, _ = lifecycle
    await bootstrap(engine)
    entered, release = threading.Event(), threading.Event()
    original_sync = broker.sync_account

    def query():
        entered.set()
        assert release.wait(5)
        return original_sync()

    monkeypatch.setattr(broker, "sync_account", query)
    broker.events.clear()
    query_task = asyncio.create_task(engine._account_sync_step())
    engine._background_tasks.append(query_task)
    shutdown_task = None
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        if already_cancelled:
            query_task.cancel()
            await asyncio.sleep(0)
        shutdown_task = asyncio.create_task(engine._shutdown())
        await asyncio.sleep(0.05)
        assert not shutdown_task.done()
        assert broker.is_connected()
    finally:
        release.set()
        if shutdown_task is not None:
            await shutdown_task
    assert query_task.cancelled()
    assert broker.events == ["query", "query", "disconnect"]


@pytest.mark.asyncio
async def test_cancelled_initialize_finishes_before_cleanup_without_saving(lifecycle):
    engine, broker, loader, _ = lifecycle
    entered, release = threading.Event(), threading.Event()

    def initialize(context):
        entered.set()
        assert release.wait(5)
        assert broker.is_connected()
        broker.events.append("initialize-finished")

    loader.initialize_func = initialize
    task = asyncio.create_task(engine.start())
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        task.cancel()
        await asyncio.sleep(0.05)
        assert not task.done()
        assert broker.is_connected()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert broker.events[-2:] == ["initialize-finished", "disconnect"]
    assert not (engine.strategy_path.parent / "runtime" / "subportfolios.json").exists()


@pytest.mark.asyncio
async def test_blocking_hook_can_itself_signal_cancellation(lifecycle):
    engine, _, _, _ = lifecycle
    engine._loop = asyncio.get_running_loop()

    def cancel():
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await engine._run_blocking(cancel)


@pytest.mark.parametrize("response", [{}, {"dtype": "dict", "value": {}}, {"value": None}])
def test_remote_account_rejects_empty_responses(response):
    broker = RemoteQmtBroker.__new__(RemoteQmtBroker)
    broker.account_key, broker.sub_account_id = "offline", None
    broker._connection = SimpleNamespace(request=lambda *args: response)
    with pytest.raises(RuntimeError, match="账户查询返回空数据"):
        broker.sync_account()


@pytest.mark.parametrize("positions_fail", [False, True])
def test_remote_positions_failure_is_not_reported_as_empty_holdings(positions_fail):
    broker = RemoteQmtBroker.__new__(RemoteQmtBroker)
    broker.account_key, broker.sub_account_id = "offline", None

    def request(action, payload):
        if action == "broker.account":
            return {"dtype": "dict", "value": {"available_cash": 0.0, "total_value": 0.0}}
        if positions_fail:
            raise TimeoutError("positions timeout")
        return []

    broker._connection = SimpleNamespace(request=request)
    if positions_fail:
        with pytest.raises(TimeoutError, match="positions timeout"):
            broker.sync_account()
    else:
        assert broker.sync_account() == {"available_cash": 0.0, "total_value": 0.0, "positions": []}
