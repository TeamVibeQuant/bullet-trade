"""
LiveEngine 核心行为测试。
"""

from __future__ import annotations

import asyncio
import json
import shutil
from datetime import date, datetime, timedelta, time as Time
from pathlib import Path

import pytest


from bullet_trade.broker.base import BrokerBase
from bullet_trade.core import pricing
from bullet_trade.core.async_scheduler import AsyncScheduler
from bullet_trade.core.event_bus import EventBus
from bullet_trade.core.live_engine import LiveEngine, LivePortfolioProxy, TradingCalendarGuard
from bullet_trade.core.live_runtime import (
    init_live_runtime,
    load_subscription_state,
    register_portfolio,
    register_portfolio_price_refresher,
    save_g,
)
from bullet_trade.core.globals import g, reset_globals
from bullet_trade.core.models import Position, SubPortfolio
from bullet_trade.core.orders import order, order_target_value, clear_order_queue
from bullet_trade.core.runtime import set_current_engine


class DummyBroker(BrokerBase):
    def __init__(self):
        super().__init__("dummy")
        self.orders: list[tuple[str, int, float, str, bool]] = []
        self.account_sync_calls = 0
        self.order_sync_calls = 0
        self.heartbeat_calls = 0
        self._tick_snapshots: dict[str, dict] = {}

    def connect(self) -> bool:
        self._connected = True
        return True

    def disconnect(self) -> bool:
        self._connected = False
        return True

    def get_account_info(self):
        return {"account_id": "dummy", "account_type": "stock", "positions": [], "available_cash": 1000.0, "total_value": 1000.0}

    def get_positions(self):
        return []

    async def buy(
        self,
        security: str,
        amount: int,
        price: float | None = None,
        wait_timeout: float | None = None,
        *,
        market: bool = False,
    ) -> str:
        self.orders.append((security, amount, price or 0.0, "buy", market))
        return f"buy-{len(self.orders)}"

    async def sell(
        self,
        security: str,
        amount: int,
        price: float | None = None,
        wait_timeout: float | None = None,
        *,
        market: bool = False,
    ) -> str:
        self.orders.append((security, amount, price or 0.0, "sell", market))
        return f"sell-{len(self.orders)}"

    async def cancel_order(self, order_id: str) -> bool:
        return True

    async def get_order_status(self, order_id: str):
        return {}

    def supports_account_sync(self) -> bool:
        return True

    def supports_orders_sync(self) -> bool:
        return True

    def supports_tick_subscription(self) -> bool:
        return True

    def sync_account(self):
        self.account_sync_calls += 1
        return {
            "available_cash": 888.0,
            "total_value": 999.0,
            "positions": [
                {"security": "000001.XSHE", "amount": 100, "avg_cost": 10.0, "current_price": 11.0, "market_value": 1100.0}
            ],
        }

    def sync_orders(self):
        self.order_sync_calls += 1
        return []

    def heartbeat(self):
        self.heartbeat_calls += 1

    def subscribe_ticks(self, symbols):
        for sym in symbols:
            self._tick_snapshots[sym] = {"sid": sym, "last_price": 1.23, "dt": datetime.now().isoformat()}

    def unsubscribe_ticks(self, symbols=None):
        if not symbols:
            self._tick_snapshots.clear()
            return
        for sym in symbols:
            self._tick_snapshots.pop(sym, None)

    def get_current_tick(self, symbol: str):
        return self._tick_snapshots.get(symbol)


def _write_strategy(tmp_path: Path) -> Path:
    src = """
from bullet_trade.core.scheduler import run_daily

def initialize(context):
    def every_minute(ctx):
        ctx.minute_calls = getattr(ctx, 'minute_calls', 0) + 1
    run_daily(every_minute, "every_minute")

def handle_data(context, data):
    context.handle_called = getattr(context, 'handle_called', 0) + 1
"""
    path = tmp_path / "strategy_live.py"
    path.write_text(src, encoding="utf-8")
    return path


def _write_strategy_with_hooks(tmp_path: Path, marker: int) -> Path:
    src = f"""
from bullet_trade.core.scheduler import run_daily
from bullet_trade.core.globals import g

def initialize(context):
    g.init_calls = (getattr(g, 'init_calls', 0) or 0) + 1
    run_daily(every_minute, "every_minute")

def process_initialize(context):
    g.proc_calls = (getattr(g, 'proc_calls', 0) or 0) + 1

def after_code_changed(context):
    g.after_calls = (getattr(g, 'after_calls', 0) or 0) + 1
    g.code_marker = {marker}

def every_minute(context):
    pass
"""
    path = tmp_path / "strategy_hooks.py"
    path.write_text(src, encoding="utf-8")
    return path


@pytest.mark.asyncio
async def test_live_engine_respects_market_session(tmp_path, monkeypatch):
    strategy = _write_strategy(tmp_path)
    runtime_dir = tmp_path / "runtime"
    cfg = {
        "runtime_dir": str(runtime_dir),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 1, 9, 0, 0),
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()

    await engine._bootstrap()
    assert isinstance(engine.context.portfolio, LivePortfolioProxy)
    await engine._ensure_trading_day(date(2025, 1, 2))

    # 09:31 -> 正常触发
    await engine._handle_minute_tick(datetime(2025, 1, 2, 9, 31))
    assert getattr(engine.context, "handle_called", 0) == 1
    assert getattr(engine.context, "minute_calls", 0) == 1

    # 窗口结束 11:30 不应触发
    await engine._handle_minute_tick(datetime(2025, 1, 2, 11, 30))
    assert getattr(engine.context, "minute_calls", 0) == 1

    # 午休 11:31 -> 不触发
    await engine._handle_minute_tick(datetime(2025, 1, 2, 11, 31))
    assert getattr(engine.context, "handle_called", 0) == 1
    assert getattr(engine.context, "minute_calls", 0) == 1

    # 下午 13:01 -> 再次触发
    await engine._handle_minute_tick(datetime(2025, 1, 2, 13, 1))
    assert getattr(engine.context, "handle_called", 0) == 2
    assert getattr(engine.context, "minute_calls", 0) == 2

    # 收盘 18:00 不触发
    await engine._handle_minute_tick(datetime(2025, 1, 2, 18, 0))
    assert getattr(engine.context, "minute_calls", 0) == 2

    await engine._shutdown()


@pytest.mark.asyncio
async def test_tick_subscription_and_account_sync(tmp_path):
    strategy = _write_strategy(tmp_path)
    runtime_dir = tmp_path / "runtime"
    cfg = {
        "runtime_dir": str(runtime_dir),
        "g_autosave_enabled": False,
        "account_sync_interval": 1,
        "account_sync_enabled": True,
        "order_sync_enabled": False,
        "tick_subscription_limit": 2,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 1, 9, 0, 0),
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()

    await engine._bootstrap()

    # tick 订阅写入 runtime
    engine.register_tick_subscription(["000001.XSHE"], [])
    symbols, markets = load_subscription_state()
    assert "000001.XSHE" in symbols

    with pytest.raises(ValueError):
        engine.register_tick_subscription(["000002.XSHE", "000003.XSHE"], [])

    # 手动触发账户同步
    await engine._account_sync_step()
    assert engine.context.portfolio.available_cash == 888.0
    assert "000001.XSHE" in engine.context.portfolio.positions

    await engine._shutdown()


@pytest.mark.asyncio
async def test_scheduler_resets_future_cursor(tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
    }
    frozen_now = datetime(2025, 1, 2, 10, 0)

    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: frozen_now,
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()

    await engine._bootstrap()
    await engine._ensure_trading_day(date(2025, 1, 2))

    # 模拟残留游标在未来
    engine._last_schedule_dt = datetime(2025, 1, 2, 12, 0)
    await engine._handle_minute_tick(datetime(2025, 1, 2, 10, 0, 5))

    assert engine.context.current_dt == datetime(2025, 1, 2, 10, 0)
    assert engine._last_schedule_dt == datetime(2025, 1, 2, 10, 0)

    await engine._shutdown()


@pytest.mark.asyncio
async def test_portfolio_proxy_refresh_on_access(tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
    }
    broker = DummyBroker()
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=lambda: broker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 2, 9, 45),
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()

    await engine._bootstrap()
    broker.account_sync_calls = 0
    _ = engine.context.portfolio.available_cash
    assert broker.account_sync_calls >= 1
    assert engine.context.portfolio.available_cash == 888.0
    assert "000001.XSHE" in engine.context.portfolio.positions
    await engine._shutdown()


@pytest.mark.asyncio
async def test_initialize_skipped_and_after_code_changed(tmp_path):
    runtime_dir = tmp_path / "runtime"
    strategy = _write_strategy_with_hooks(tmp_path, marker=1)
    cfg = {
        "runtime_dir": str(runtime_dir),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
    }

    loop = asyncio.get_running_loop()

    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 2, 9, 0),
    )
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()
    await engine._bootstrap()
    save_g()
    await engine._shutdown()

    assert (getattr(g, "init_calls", 0) or 0) == 1
    assert (getattr(g, "proc_calls", 0) or 0) == 1
    assert (getattr(g, "after_calls", 0) or 0) == 0

    strategy = _write_strategy_with_hooks(tmp_path, marker=2)

    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 2, 9, 5),
    )
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()
    await engine._bootstrap()
    save_g()
    await engine._shutdown()

    assert (getattr(g, "init_calls", 0) or 0) == 1
    assert (getattr(g, "proc_calls", 0) or 0) == 2
    assert (getattr(g, "after_calls", 0) or 0) == 1

    reset_globals()
    shutil.rmtree(runtime_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_calendar_guard_skips_weekend(monkeypatch):
    guard = TradingCalendarGuard({"calendar_skip_weekend": True, "calendar_retry_minutes": 15})

    def _raise(*args, **kwargs):
        raise RuntimeError("no data")

    monkeypatch.setattr("bullet_trade.core.live_engine.get_trade_days", _raise, raising=False)
    saturday = datetime(2025, 1, 4, 9, 0)
    result = await guard.ensure_trade_day(saturday)
    assert result is False
    assert guard._next_check == saturday + timedelta(minutes=15)

    monday = datetime(2025, 1, 6, 9, 0)
    result = await guard.ensure_trade_day(monday)
    assert result is True
    assert guard._confirmed_date == monday.date()


@pytest.mark.asyncio
async def test_calendar_guard_weekend_allowed(monkeypatch):
    guard = TradingCalendarGuard({"calendar_skip_weekend": False})

    def _raise(*args, **kwargs):
        raise RuntimeError("no data")

    monkeypatch.setattr("bullet_trade.core.live_engine.get_trade_days", _raise, raising=False)
    sunday = datetime(2025, 1, 5, 9, 0)
    result = await guard.ensure_trade_day(sunday)
    assert result is True
    assert guard._confirmed_date == sunday.date()


@pytest.mark.asyncio
async def test_live_engine_skips_processed_minute_after_restart(tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 2, 9, 0),
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()

    await engine._bootstrap()
    await engine._ensure_trading_day(date(2025, 1, 2))
    engine._last_schedule_dt = datetime(2025, 1, 2, 9, 40)
    original_dt = engine.context.current_dt

    await engine._handle_minute_tick(datetime(2025, 1, 2, 9, 40, 20))
    assert engine._last_schedule_dt == datetime(2025, 1, 2, 9, 40)
    assert engine.context.current_dt == original_dt

    await engine._handle_minute_tick(datetime(2025, 1, 2, 9, 41, 0))
    assert engine._last_schedule_dt == datetime(2025, 1, 2, 9, 41)
    assert engine.context.current_dt == datetime(2025, 1, 2, 9, 41)
    await engine._shutdown()


@pytest.mark.asyncio
async def test_live_engine_event_timeout_drops_minute(tmp_path, caplog):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
        "event_time_out": 5,
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 2, 9, 0),
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()
    await engine._bootstrap()
    await engine._ensure_trading_day(date(2025, 1, 2))

    def _fail_handle(*_args, **_kwargs):
        raise AssertionError("handle_data should be skipped when timed-out")

    engine.handle_data_func = _fail_handle
    caplog.set_level("WARNING", logger="jq_strategy")
    await engine._handle_minute_tick(datetime(2025, 1, 2, 9, 31, 10))
    assert "事件超时丢弃" in caplog.text
    assert engine._last_schedule_dt == datetime(2025, 1, 2, 9, 31)
    await engine._shutdown()


@pytest.mark.asyncio
async def test_live_engine_applies_scheduler_override_from_env(tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "08:00-09:00,10:00-10:30",
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 2, 7, 50),
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()
    await engine._bootstrap()
    await engine._ensure_trading_day(date(2025, 1, 2))
    assert engine._market_periods == [(Time(8, 0), Time(9, 0)), (Time(10, 0), Time(10, 30))]
    await engine._shutdown()


@pytest.mark.asyncio
async def test_live_engine_tick_snapshot_and_unsubscribe(tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 2, 9, 0),
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()
    await engine._bootstrap()
    engine._latest_ticks["000001.XSHE"] = {"sid": "000001.XSHE", "last_price": 10.5}
    snap = engine.get_current_tick_snapshot("000001.XSHE")
    assert snap["last_price"] == 10.5

    engine._latest_ticks.clear()
    engine.broker._tick_snapshots["000001.XSHE"] = {"sid": "000001.XSHE", "last_price": 11.0}  # type: ignore[attr-defined]
    snap = engine.get_current_tick_snapshot("000001.XSHE")
    assert snap["last_price"] == 11.0

    engine.register_tick_subscription(["000001.XSHE"], [])
    assert "000001.XSHE" in engine._tick_symbols
    engine.unsubscribe_all_ticks()
    assert not engine._tick_symbols
    assert not engine._tick_markets
    await engine._shutdown()


@pytest.mark.asyncio
async def test_handle_tick_hook_receives_context(tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
        "scheduler_market_periods": "09:30-11:30,13:00-15:00",
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
        now_provider=lambda: datetime(2025, 1, 2, 9, 0),
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()

    await engine._bootstrap()
    payload = {}

    async def _handler(ctx, tick):
        payload["context"] = ctx
        payload["tick"] = tick

    engine.handle_tick_func = _handler
    await engine._call_hook(engine.handle_tick_func, {"sid": "000001.XSHE"})
    assert payload["context"] is engine.context
    assert payload["tick"]["sid"] == "000001.XSHE"
    await engine._shutdown()


@pytest.mark.asyncio
async def test_market_flag_propagates_to_broker(monkeypatch, tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
    )
    engine.broker = DummyBroker()
    engine.context.portfolio.available_cash = 1_000_000
    engine.context.portfolio.total_value = 1_000_000
    engine._risk = None

    class Snap:
        paused = False
        last_price = 10.0
        high_limit = 10.5
        low_limit = 9.5

    monkeypatch.setattr("bullet_trade.core.live_engine.get_current_data", lambda: {"000001.XSHE": Snap()})

    clear_order_queue()
    order("000001.XSHE", 100)
    order("000001.XSHE", 100, price=10.5)

    await engine._process_orders(engine.context.current_dt)

    assert len(engine.broker.orders) == 2

    sec1, amt1, price1, side1, market1 = engine.broker.orders[0]
    assert (sec1, amt1, side1, market1) == ("000001.XSHE", 100, "buy", True)
    expected_price = pricing.compute_market_protect_price("000001.XSHE", 10.0, 10.5, 9.5, 0.015, True)
    assert price1 == pytest.approx(expected_price)

    sec2, amt2, price2, side2, market2 = engine.broker.orders[1]
    assert (sec2, amt2, side2, market2) == ("000001.XSHE", 100, "buy", False)
    assert price2 == pytest.approx(10.5)


@pytest.mark.asyncio
async def test_live_order_updates_virtual_subportfolio_by_pindex(monkeypatch, tmp_path):
    strategy = _write_strategy(tmp_path)
    runtime_dir = tmp_path / "runtime"
    cfg = {
        "runtime_dir": str(runtime_dir),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
    )
    engine.broker = DummyBroker()
    engine._risk = None
    init_live_runtime(str(runtime_dir))
    register_portfolio(engine._portfolio)
    engine._portfolio.subportfolios.clear()
    engine._portfolio.subportfolios[0] = SubPortfolio(
        type="stock",
        available_cash=6000.0,
        transferable_cash=6000.0,
        total_value=6000.0,
    )
    engine._portfolio.subportfolios[1] = SubPortfolio(
        type="stock",
        available_cash=4000.0,
        transferable_cash=4000.0,
        total_value=4000.0,
    )
    engine._portfolio.update_value()

    class Snap:
        paused = False
        last_price = 10.0
        high_limit = 10.5
        low_limit = 9.5

    monkeypatch.setattr("bullet_trade.core.live_engine.get_current_data", lambda: {"000001.XSHE": Snap()})

    clear_order_queue()
    set_current_engine(None)
    order_target_value("000001.XSHE", 2000.0, pindex=1)
    await engine._process_orders(engine.context.current_dt)

    assert "000001.XSHE" not in engine._portfolio.subportfolios[0].positions
    pos = engine._portfolio.subportfolios[1].positions["000001.XSHE"]
    assert pos.total_amount == 200
    buy_price = pricing.compute_market_protect_price("000001.XSHE", 10.0, 10.5, 9.5, 0.015, True)
    assert engine._portfolio.subportfolios[1].available_cash == pytest.approx(4000.0 - 200 * buy_price)
    assert engine.context.subportfolios[1].positions["000001.XSHE"].total_amount == 200

    clear_order_queue()
    order_target_value("000001.XSHE", 1000.0, pindex=0)
    await engine._process_orders(engine.context.current_dt)

    assert engine.context.subportfolios[0].positions["000001.XSHE"].total_amount == 100
    assert engine.context.subportfolios[1].positions["000001.XSHE"].total_amount == 200

    snapshot_path = runtime_dir / "subportfolios.json"
    assert snapshot_path.exists()
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    assert snapshot["subportfolios"]["0"]["positions"]["000001.XSHE"]["total_amount"] == 100
    assert snapshot["subportfolios"]["1"]["positions"]["000001.XSHE"]["total_amount"] == 200

    clear_order_queue()
    order_target_value("000001.XSHE", 0.0, pindex=1)
    await engine._process_orders(engine.context.current_dt)

    assert "000001.XSHE" in engine._portfolio.subportfolios[0].positions
    assert "000001.XSHE" not in engine._portfolio.subportfolios[1].positions


@pytest.mark.asyncio
async def test_restore_subportfolios_refreshes_prices_from_broker_snapshot(tmp_path):
    strategy = _write_strategy(tmp_path)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    snapshot_path = runtime_dir / "subportfolios.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "saved_at": "2025-01-02T09:30:00",
                "subportfolios": {
                    "0": {
                        "type": "stock",
                        "available_cash": 1000.0,
                        "transferable_cash": 1000.0,
                        "locked_cash": 0.0,
                        "total_value": 1200.0,
                        "positions": {
                            "000001.XSHE": {
                                "security": "000001.XSHE",
                                "total_amount": 40,
                                "closeable_amount": 40,
                                "avg_cost": 9.0,
                                "price": 5.0,
                                "acc_avg_cost": 9.0,
                                "value": 200.0,
                                "side": "long",
                            }
                        },
                    },
                    "1": {
                        "type": "stock",
                        "available_cash": 2000.0,
                        "transferable_cash": 2000.0,
                        "locked_cash": 0.0,
                        "total_value": 2360.0,
                        "positions": {
                            "000001.XSHE": {
                                "security": "000001.XSHE",
                                "total_amount": 60,
                                "closeable_amount": 60,
                                "avg_cost": 9.5,
                                "price": 6.0,
                                "acc_avg_cost": 9.5,
                                "value": 360.0,
                                "side": "long",
                            }
                        },
                    },
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config={
            "runtime_dir": str(runtime_dir),
            "g_autosave_enabled": False,
            "account_sync_enabled": False,
            "order_sync_enabled": False,
            "tick_sync_enabled": False,
            "risk_check_enabled": False,
            "broker_heartbeat_interval": 0,
        },
    )
    engine.broker = DummyBroker()
    init_live_runtime(str(runtime_dir))
    register_portfolio(engine._portfolio)

    engine._restore_subportfolios()

    pos0 = engine.context.subportfolios[0].positions["000001.XSHE"]
    pos1 = engine.context.subportfolios[1].positions["000001.XSHE"]
    assert pos0.price == pytest.approx(11.0)
    assert pos0.value == pytest.approx(440.0)
    assert pos0.avg_cost == pytest.approx(10.0)
    assert pos0.acc_avg_cost == pytest.approx(10.0)
    assert pos1.price == pytest.approx(11.0)
    assert pos1.value == pytest.approx(660.0)
    assert pos1.avg_cost == pytest.approx(10.0)
    assert pos1.acc_avg_cost == pytest.approx(10.0)
    assert engine.context.subportfolios[0].total_value == pytest.approx(1440.0)
    assert engine.context.subportfolios[1].total_value == pytest.approx(2660.0)

    refreshed_snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    assert refreshed_snapshot["subportfolios"]["0"]["positions"]["000001.XSHE"]["price"] == pytest.approx(11.0)
    assert refreshed_snapshot["subportfolios"]["0"]["positions"]["000001.XSHE"]["avg_cost"] == pytest.approx(10.0)
    assert refreshed_snapshot["subportfolios"]["1"]["positions"]["000001.XSHE"]["value"] == pytest.approx(660.0)
    assert refreshed_snapshot["subportfolios"]["1"]["positions"]["000001.XSHE"]["acc_avg_cost"] == pytest.approx(10.0)


def test_save_g_refreshes_subportfolio_prices_before_writing(tmp_path):
    strategy = _write_strategy(tmp_path)
    runtime_dir = tmp_path / "runtime"
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config={
            "runtime_dir": str(runtime_dir),
            "g_autosave_enabled": False,
            "account_sync_enabled": False,
            "order_sync_enabled": False,
            "tick_sync_enabled": False,
            "risk_check_enabled": False,
            "broker_heartbeat_interval": 0,
        },
    )
    engine.broker = DummyBroker()
    init_live_runtime(str(runtime_dir))
    register_portfolio(engine._portfolio)
    register_portfolio_price_refresher(
        lambda force_log=False: engine._refresh_subportfolio_prices(engine._portfolio, force_log=force_log)
    )

    engine._portfolio.subportfolios.clear()
    engine._portfolio.subportfolios[0] = SubPortfolio(
        type="stock",
        available_cash=1000.0,
        transferable_cash=1000.0,
        total_value=1200.0,
        positions={
            "000001.XSHE": Position(
                security="000001.XSHE",
                total_amount=20,
                closeable_amount=20,
                avg_cost=9.0,
                price=5.0,
                value=100.0,
            )
        },
    )
    engine._portfolio.subportfolios[1] = SubPortfolio(
        type="stock",
        available_cash=2000.0,
        transferable_cash=2000.0,
        total_value=2300.0,
        positions={
            "000001.XSHE": Position(
                security="000001.XSHE",
                total_amount=30,
                closeable_amount=30,
                avg_cost=9.5,
                price=6.0,
                value=180.0,
            )
        },
    )

    save_g()

    snapshot = json.loads((runtime_dir / "subportfolios.json").read_text(encoding="utf-8"))
    assert snapshot["subportfolios"]["0"]["positions"]["000001.XSHE"]["price"] == pytest.approx(11.0)
    assert snapshot["subportfolios"]["0"]["positions"]["000001.XSHE"]["value"] == pytest.approx(220.0)
    assert snapshot["subportfolios"]["0"]["positions"]["000001.XSHE"]["avg_cost"] == pytest.approx(10.0)
    assert snapshot["subportfolios"]["0"]["positions"]["000001.XSHE"]["acc_avg_cost"] == pytest.approx(10.0)
    assert snapshot["subportfolios"]["1"]["positions"]["000001.XSHE"]["price"] == pytest.approx(11.0)
    assert snapshot["subportfolios"]["1"]["positions"]["000001.XSHE"]["value"] == pytest.approx(330.0)
    assert snapshot["subportfolios"]["1"]["positions"]["000001.XSHE"]["avg_cost"] == pytest.approx(10.0)
    assert snapshot["subportfolios"]["1"]["positions"]["000001.XSHE"]["acc_avg_cost"] == pytest.approx(10.0)


def test_account_snapshot_refreshes_virtual_subportfolio_costs(tmp_path):
    strategy = _write_strategy(tmp_path)
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config={
            "runtime_dir": str(tmp_path / "runtime"),
            "g_autosave_enabled": False,
            "account_sync_enabled": False,
            "order_sync_enabled": False,
            "tick_sync_enabled": False,
            "risk_check_enabled": False,
            "broker_heartbeat_interval": 0,
        },
    )
    engine._portfolio.subportfolios.clear()
    engine._portfolio.subportfolios[0] = SubPortfolio(
        type="stock",
        available_cash=1000.0,
        transferable_cash=1000.0,
        total_value=1200.0,
        positions={
            "000001.XSHE": Position(
                security="000001.XSHE",
                total_amount=20,
                closeable_amount=20,
                avg_cost=8.0,
                acc_avg_cost=8.0,
                price=5.0,
                value=100.0,
            )
        },
    )
    engine._portfolio.subportfolios[1] = SubPortfolio(
        type="stock",
        available_cash=2000.0,
        transferable_cash=2000.0,
        total_value=2300.0,
        positions={
            "000001.XSHE": Position(
                security="000001.XSHE",
                total_amount=30,
                closeable_amount=30,
                avg_cost=8.5,
                acc_avg_cost=8.5,
                price=6.0,
                value=180.0,
            )
        },
    )

    engine._apply_account_snapshot(
        {
            "available_cash": 5000.0,
            "total_value": 6000.0,
            "positions": [
                {
                    "security": "000001.XSHE",
                    "amount": 50,
                    "avg_cost": 12.0,
                    "current_price": 13.0,
                    "market_value": 650.0,
                }
            ],
        }
    )

    pos0 = engine.context.subportfolios[0].positions["000001.XSHE"]
    pos1 = engine.context.subportfolios[1].positions["000001.XSHE"]
    assert pos0.avg_cost == pytest.approx(12.0)
    assert pos0.acc_avg_cost == pytest.approx(12.0)
    assert pos0.price == pytest.approx(13.0)
    assert pos0.value == pytest.approx(260.0)
    assert pos1.avg_cost == pytest.approx(12.0)
    assert pos1.acc_avg_cost == pytest.approx(12.0)
    assert pos1.price == pytest.approx(13.0)
    assert pos1.value == pytest.approx(390.0)


@pytest.mark.asyncio
async def test_process_orders_runs_once_with_lock(monkeypatch, tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._order_lock = asyncio.Lock()
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()
    engine.broker = DummyBroker()
    engine._risk = None
    engine.context.portfolio.available_cash = 1_000_000
    engine.context.portfolio.total_value = 1_000_000
    set_current_engine(engine)

    class Snap:
        paused = False
        last_price = 10.0
        high_limit = 10.5
        low_limit = 9.5

    monkeypatch.setattr("bullet_trade.core.live_engine.get_current_data", lambda: {"000001.XSHE": Snap()})

    clear_order_queue()
    order("000001.XSHE", 100, wait_timeout=0)

    task1 = asyncio.create_task(engine._process_orders(engine.context.current_dt))
    task2 = asyncio.create_task(engine._process_orders(engine.context.current_dt))
    await asyncio.gather(task1, task2)
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert len(engine.broker.orders) == 1
    set_current_engine(None)


@pytest.mark.asyncio
async def test_order_waits_until_processed(monkeypatch, tmp_path):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
        "g_autosave_enabled": False,
        "account_sync_enabled": False,
        "order_sync_enabled": False,
        "tick_sync_enabled": False,
        "risk_check_enabled": False,
        "broker_heartbeat_interval": 0,
    }
    gate = asyncio.Event()

    class SlowBroker(DummyBroker):
        def __init__(self, signal: asyncio.Event):
            super().__init__()
            self.signal = signal

        async def buy(
            self,
            security: str,
            amount: int,
            price: float | None = None,
            wait_timeout: float | None = None,
            *,
            market: bool = False,
        ) -> str:
            await self.signal.wait()
            return await super().buy(security, amount, price, wait_timeout=wait_timeout, market=market)

    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=SlowBroker,
        live_config=cfg,
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._order_lock = asyncio.Lock()
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()
    engine.broker = SlowBroker(gate)
    engine._risk = None
    engine.context.portfolio.available_cash = 1_000_000
    engine.context.portfolio.total_value = 1_000_000
    set_current_engine(engine)

    class Snap:
        paused = False
        last_price = 10.0
        high_limit = 10.5
        low_limit = 9.5

    monkeypatch.setattr("bullet_trade.core.live_engine.get_current_data", lambda: {"000001.XSHE": Snap()})

    clear_order_queue()

    async def _run_order():
        return await asyncio.to_thread(order, "000001.XSHE", 100)

    order_task = asyncio.create_task(_run_order())
    await asyncio.sleep(0)
    assert len(engine.broker.orders) == 0
    gate.set()
    await order_task
    assert len(engine.broker.orders) == 1
    set_current_engine(None)


def test_live_engine_run_returns_nonzero_on_error(tmp_path, monkeypatch, caplog):
    strategy = _write_strategy(tmp_path)
    cfg = {
        "runtime_dir": str(tmp_path / "runtime"),
    }
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=DummyBroker,
        live_config=cfg,
    )

    async def _boom(self):
        raise RuntimeError("missing xtquant")

    monkeypatch.setattr(LiveEngine, "start", _boom)
    caplog.set_level("ERROR", logger="jq_strategy")
    exit_code = engine.run()
    assert exit_code == 2
    assert "missing xtquant" in caplog.text
