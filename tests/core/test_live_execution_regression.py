"""Offline reproductions of the 2026-11-05 cash/late-fill incident."""
from __future__ import annotations

import asyncio
import copy
import json
from dataclasses import asdict
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from bullet_trade.broker.base import BrokerBase
from bullet_trade.core.globals import reset_globals
from bullet_trade.core.live_engine import LiveEngine, _PendingVirtualOrder, _ResolvedOrder
from bullet_trade.core.live_runtime import (
    init_live_runtime, close_live_runtime, register_portfolio, register_execution_state, save_g,
)
from bullet_trade.core.models import Order, Position, SubPortfolio
from bullet_trade.core.orders import order, clear_order_queue, LimitOrderStyle
from bullet_trade.core.runtime import set_current_engine


class Broker(BrokerBase):
    def __init__(self):
        super().__init__("offline")
        self._connected = True
        self.statuses = {}
        self.submitted = []
        self.snapshot = {}
        self.reject = False

    def connect(self): return True
    def disconnect(self): return True
    def get_account_info(self): return self.snapshot
    def get_positions(self): return self.snapshot.get("positions", [])
    def supports_account_sync(self): return True
    def supports_orders_sync(self): return True
    def sync_account(self): return copy.deepcopy(self.snapshot)
    def sync_orders(self): return self.get_orders()
    def get_orders(self, **kwargs):
        return [dict(s, order_id=k) for k, s in self.statuses.items()]
    async def buy(self, security, amount, price=None, **kwargs):
        self.submitted.append((security, amount, price))
        if self.reject:
            raise RuntimeError("status=rejected [260200][可用资金不足]")
        key = str(len(self.submitted))
        self.statuses[key] = {"security": security, "status": "filling", "filled": 0, "price": 0}
        return key
    async def sell(self, *args, **kwargs): return await self.buy(*args, **kwargs)
    async def get_order_status(self, key): return self.statuses.get(key, {})
    async def cancel_order(self, key): return True


@pytest.fixture
def engine(tmp_path, monkeypatch):
    strategy = tmp_path / "strategy.py"
    strategy.write_text("def initialize(context): pass\n", encoding="utf-8")
    eng = LiveEngine(strategy, broker_factory=Broker, live_config={
        "runtime_dir": str(tmp_path / "runtime"), "g_autosave_enabled": False,
        "account_sync_enabled": False, "order_sync_enabled": False,
        "tick_sync_enabled": False, "risk_check_enabled": False, "broker_heartbeat_interval": 0,
    })
    eng.broker = Broker()
    eng._risk = None
    eng.context.current_dt = datetime(2026, 11, 5, 9, 34)
    eng._portfolio.subportfolios = {
        0: SubPortfolio(available_cash=0),
        1: SubPortfolio(available_cash=100_000, transferable_cash=100_000),
    }
    eng._portfolio.update_value()
    eng.broker.snapshot = {"available_cash": 100_000, "total_value": 100_000, "positions": []}
    init_live_runtime(str(tmp_path / "runtime"))
    register_portfolio(eng._portfolio)
    register_execution_state(eng._execution.export, eng._execution_lock)
    ticks = {s: SimpleNamespace(last_price=10.0, high_limit=11.0, low_limit=9.0, paused=False)
             for s in ("000001.XSHE", "000002.XSHE")}
    monkeypatch.setattr("bullet_trade.core.live_engine.get_current_data", lambda: ticks)
    yield eng
    close_live_runtime(save=False)
    set_current_engine(None)
    clear_order_queue()
    reset_globals()


def make_order(engine, key="order1", security="000001.XSHE", amount=1000, price=10.0, is_buy=True):
    obj = Order(order_id=key, security=security, amount=amount, is_buy=is_buy, pindex=1)
    plan = _ResolvedOrder(security, amount, is_buy, price, price, 1, 0, False)
    engine._execution.reserve(key, asdict(plan), datetime.now().isoformat())
    return obj, plan


def snapshot(engine, cash=None, *, total=None):
    sp = engine._portfolio.subportfolios[1]
    positions = [{"security": s, "amount": p.total_amount, "current_price": p.price,
                  "market_value": p.value, "avg_cost": p.avg_cost}
                 for s, p in sp.positions.items()]
    cash = sp.available_cash if cash is None else cash
    return {"available_cash": cash, "positions": positions,
            "total_value": cash + sum(p["market_value"] for p in positions) if total is None else total}


def test_nov05_old_cash_is_not_a_deposit_after_30_seconds(engine):
    sp = engine._portfolio.subportfolios[1]
    sp.available_cash = 11_434_970.20
    sp.positions["600183.XSHG"] = Position(security="600183.XSHG", total_amount=100,
                                          price=82908.1568, value=8_290_815.68)
    before_nav = sp.available_cash + sp.positions["600183.XSHG"].value
    spends = [2_532_581.6, 2_519_598.5, 2_525_514.0, 2_536_434.88]
    for i, value in enumerate(spends):
        obj, plan = make_order(engine, str(i), "new%d.XSHE" % i, 100, value / 100)
        engine._apply_virtual_order_fill(obj, plan, {"status": "filled", "filled": 100, "price": value / 100})
    assert sp.available_cash == pytest.approx(1_320_841.22)
    engine._suppress_cash_reconcile_until = datetime.now() - timedelta(minutes=10)
    old = snapshot(engine, cash=11_434_970.20, total=before_nav)
    for _ in range(4):
        engine._apply_account_snapshot(old)
    assert sp.available_cash == pytest.approx(1_320_841.22)
    assert engine._portfolio.total_value == pytest.approx(before_nav)
    assert engine.get_execution_budget(1)["blocked"]
    assert engine._cap_buy_amount_by_available_cash("000001.XSHE", 10000, 10, 10, 11, False, 1) == 0
    # A consistent cash update (including actual fees) releases the guard.
    engine._apply_account_snapshot(snapshot(engine, cash=sp.available_cash - 2200))
    assert engine.get_execution_budget(1)["cash_ready"]
    assert sp.available_cash == pytest.approx(1_318_641.22)


def test_old_positions_cannot_replace_new_fill_before_cash_catches_up(engine):
    obj, plan = make_order(engine)
    engine._apply_virtual_order_fill(obj, plan, {"status": "filled", "filled": 1000, "price": 10})
    engine._suppress_position_sync_until = datetime.now() - timedelta(minutes=10)
    engine._apply_account_snapshot({"available_cash": 100000, "total_value": 100000, "positions": []})
    assert engine._portfolio.subportfolios[1].positions[obj.security].total_amount == 1000
    assert engine._execution.block_reason == "positions_before_fill"


def test_partial_fill_cancel_and_duplicate_reports_are_incremental(engine):
    obj, plan = make_order(engine)
    first = {"status": "filling", "filled": 300, "price": 10.0}
    assert engine._apply_virtual_order_fill(obj, plan, first)
    assert not engine._apply_virtual_order_fill(obj, plan, first)
    assert engine.get_execution_budget(1)["pending_value"] == 7000
    second = {"status": "partly_canceled", "filled": 500, "price": 10.2, "commission": 5.0}
    assert engine._apply_virtual_order_fill(obj, plan, second)
    assert not engine._apply_virtual_order_fill(obj, plan, second)
    assert not engine._apply_virtual_order_fill(obj, plan, first)
    sp = engine._portfolio.subportfolios[1]
    assert sp.available_cash == pytest.approx(100000 - 5100 - 5)
    assert sp.positions[obj.security].total_amount == 500
    assert engine.get_execution_budget(1)["reserved_cash"] == 0


def test_valid_deposit_withdrawal_and_external_freeze(engine):
    engine._apply_account_snapshot(snapshot(engine, cash=120000))
    assert engine._portfolio.subportfolios[1].available_cash == 120000
    engine._apply_account_snapshot(snapshot(engine, cash=90000))
    assert engine._portfolio.subportfolios[1].available_cash == 90000
    frozen = snapshot(engine, cash=70000, total=90000)
    frozen["frozen_cash"] = 20000
    engine._apply_account_snapshot(frozen)
    assert engine._portfolio.subportfolios[1].available_cash == 90000
    assert engine.get_execution_budget(1)["available_cash"] == 70000
    make_order(engine, amount=6000)
    assert 9900 < engine.get_execution_budget(1)["available_cash"] < 10000


def test_cancel_without_cumulative_quantity_keeps_reservation(engine):
    obj, plan = make_order(engine)
    engine._apply_virtual_order_fill(obj, plan, {"status": "canceled"})
    assert engine._execution.pending()
    assert engine.get_execution_budget(1)["reserved_cash"] > 10000


def test_incomplete_terminal_snapshot_does_not_report_completion(engine):
    obj, plan = make_order(engine)
    engine._register_order(obj)
    engine._broker_order_index["1"] = obj.order_id
    report = {"order_id": "1", "status": "filled", "filled": 0, "price": 0}
    engine._apply_virtual_order_fill(obj, plan, report)
    engine._apply_order_snapshots([report])
    assert obj.status == "settlement_pending"
    assert engine._execution.pending()


@pytest.mark.asyncio
async def test_lost_ack_is_recovered_by_unique_remark_without_resubmitting(engine):
    obj, plan = make_order(engine)
    engine._execution.orders[obj.order_id]["remark"] = "unique-local-intent"
    engine._pending_virtual_orders["unknown:" + obj.order_id] = _PendingVirtualOrder(obj, plan, datetime.now())
    engine.broker.statuses["42"] = {"status": "filled", "filled": 1000, "price": 10,
                                    "order_remark": "unique-local-intent"}
    engine._loop = asyncio.get_running_loop()
    await engine._order_sync_step()
    assert not engine._pending_virtual_orders
    assert not engine.broker.submitted
    assert engine._portfolio.subportfolios[1].positions[obj.security].total_amount == 1000


def test_counter_cash_limit_is_not_overwritten_by_old_account_query(engine):
    engine._record_buy_cash_rejection("[260200][可用资金不足][v_enable_balance_t=88834.89]")
    engine.context.current_dt += timedelta(minutes=1)
    engine._apply_account_snapshot(snapshot(engine))
    assert engine.get_execution_budget(1)["blocked"]
    engine._apply_account_snapshot(snapshot(engine, cash=88834.89))
    assert not engine.get_execution_budget(1)["blocked"]


def test_partial_sell_credits_only_executed_amount(engine):
    sp = engine._portfolio.subportfolios[1]
    sp.positions["000001.XSHE"] = Position(security="000001.XSHE", total_amount=1000,
                                          closeable_amount=1000, price=10, value=10000)
    obj, plan = make_order(engine, is_buy=False)
    engine._apply_virtual_order_fill(obj, plan, {"status": "filling", "filled": 300, "price": 10})
    assert sp.positions[obj.security].total_amount == 700
    assert sp.available_cash == 103000
    assert engine.get_execution_budget(1)["pending_sells"] == [obj.security]
    engine._apply_virtual_order_fill(obj, plan, {"status": "partly_canceled", "filled": 300, "price": 10})
    assert sp.available_cash == 103000
    assert not engine._execution.pending()


@pytest.mark.parametrize("position_amount", [0, 100])
def test_sell_position_mismatch_does_not_advance_fill_watermark(engine, position_amount):
    sp = engine._portfolio.subportfolios[1]
    security = "000001.XSHE"
    if position_amount:
        sp.positions[security] = Position(security=security, total_amount=position_amount,
                                          closeable_amount=position_amount, price=10)
    obj, plan = make_order(engine, is_buy=False)
    report = {"status": "filled", "filled": 1000, "price": 10}
    assert not engine._apply_virtual_order_fill(obj, plan, report)
    assert sp.available_cash == 100000
    assert engine._execution.orders[obj.order_id]["applied_amount"] == 0
    assert engine._execution.pending()
    assert engine.get_execution_budget(1)["blocked"]
    # Once ownership is corrected, replaying the same report applies it exactly once.
    sp.positions[security] = Position(security=security, total_amount=1000,
                                      closeable_amount=1000, price=10)
    assert engine._apply_virtual_order_fill(obj, plan, report)
    assert not engine._apply_virtual_order_fill(obj, plan, report)
    assert sp.available_cash == 110000
    assert security not in sp.positions
    assert not engine.get_execution_budget(1)["blocked"]


@pytest.mark.asyncio
@pytest.mark.parametrize("submit_seconds,fill_at,expected_filled", [(0, 12, True), (18, 25, False)])
async def test_configured_wait_budget_includes_submission_time(engine, monkeypatch, submit_seconds, fill_at, expected_filled):
    clock = [0.0]
    real_sleep = asyncio.sleep
    async def sleep(seconds):
        clock[0] += seconds
        await real_sleep(0)
    original_buy = engine.broker.buy
    async def buy(*args, **kwargs):
        clock[0] += submit_seconds
        return await original_buy(*args, **kwargs)
    async def status(key):
        if clock[0] >= fill_at:
            engine.broker.statuses[key].update(status="filled", filled=1000, price=10.0)
        return engine.broker.statuses[key]
    monkeypatch.setattr("bullet_trade.core.live_engine._monotonic", lambda: clock[0])
    monkeypatch.setattr("bullet_trade.core.live_engine.asyncio.sleep", sleep)
    monkeypatch.setattr(engine.broker, "buy", buy)
    monkeypatch.setattr(engine.broker, "get_order_status", status)
    obj = order("000001.XSHE", 1000, pindex=1, wait_timeout=20, style=LimitOrderStyle(10))
    await engine._process_orders(engine.context.current_dt)
    assert engine._execution.orders[obj.order_id]["terminal"] is expected_filled
    assert clock[0] == (12 if expected_filled else 20)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_ack", [False, True])
async def test_pending_reserves_cash_and_blocks_duplicate_security(engine, monkeypatch, missing_ack):
    original_buy = engine.broker.buy
    async def buy(*args, **kwargs):
        key = await original_buy(*args, **kwargs)
        engine.broker.statuses[key]["order_remark"] = kwargs["remark"]
        return None if missing_ack else key
    monkeypatch.setattr(engine.broker, "buy", buy)
    obj = order("000001.XSHE", 6000, pindex=1, wait_timeout=0, style=LimitOrderStyle(10))
    await engine._process_orders(engine.context.current_dt)
    state = engine.get_execution_budget(1)
    assert 39000 < state["available_cash"] < 40000
    assert state["pending_buys"] == ["000001.XSHE"]
    assert ("unknown:" + obj.order_id if missing_ack else "1") in engine._pending_virtual_orders
    order("000001.XSHE", 6000, pindex=1, wait_timeout=0, style=LimitOrderStyle(10))
    await engine._process_orders(engine.context.current_dt)
    assert len(engine.broker.submitted) == 1
    # Later terminal report is applied once by the background sync.
    engine._loop = asyncio.get_running_loop()
    engine.broker.statuses["1"].update(status="filled", filled=6000, price=10.0)
    await engine._order_sync_step()
    await engine._order_sync_step()
    assert engine._portfolio.subportfolios[1].positions["000001.XSHE"].total_amount == 6000
    assert engine._portfolio.subportfolios[1].available_cash == 40000


@pytest.mark.asyncio
async def test_rejection_stops_remaining_buy_batch(engine):
    engine.broker.reject = True
    for code in ("000001.XSHE", "000002.XSHE"):
        order(code, 1000, pindex=1, wait_timeout=0, style=LimitOrderStyle(10))
    await engine._process_orders(engine.context.current_dt)
    assert len(engine.broker.submitted) == 1
    assert engine.get_execution_budget(1)["blocked"]
    assert engine.get_execution_budget(1)["reserved_cash"] == 0


@pytest.mark.asyncio
async def test_cancel_acknowledgement_and_missing_order_do_not_release_reservation(engine):
    obj, plan = make_order(engine)
    pending = _PendingVirtualOrder(obj, plan, datetime.now() - timedelta(minutes=5))
    engine._pending_virtual_orders["missing"] = pending
    removed = await engine._expire_pending_virtual_order(
        "missing", pending, broker_visible=False, has_status_snapshot=False, status_text="unknown", elapsed_s=300)
    assert not removed
    assert "missing" in engine._pending_virtual_orders
    assert engine.get_execution_budget(1)["reserved_cash"] > 10000


def test_restart_keeps_fill_watermark_and_reservation_in_same_snapshot(engine, tmp_path):
    obj, plan = make_order(engine)
    engine._execution.orders[obj.order_id]["broker_id"] = "1"
    engine._apply_virtual_order_fill(obj, plan, {"status": "filling", "filled": 300, "price": 10})
    save_g()
    saved = json.loads((tmp_path / "runtime" / "subportfolios.json").read_text(encoding="utf-8"))
    assert saved["execution_state"]["orders"][obj.order_id]["applied_amount"] == 300
    restored = LiveEngine(engine.strategy_path, live_config={"runtime_dir": str(tmp_path / "runtime")})
    restored.broker = Broker()
    restored.broker.snapshot = snapshot(engine)
    restored._restore_subportfolios()
    assert restored.get_execution_budget(1)["reserved_cash"] > 7000
    pending = restored._pending_virtual_orders["1"]
    restored._apply_virtual_order_fill(pending.order, pending.plan, {"status": "filled", "filled": 1000, "price": 10})
    sp = restored._portfolio.subportfolios[1]
    assert sp.positions[obj.security].total_amount == 1000
    assert sp.available_cash == 90000
