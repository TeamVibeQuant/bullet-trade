"""Exercise actual strategy methods without importing its live data/alert setup."""
from __future__ import annotations

import ast
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Optional

import pytest

from bullet_trade.core import settings
from bullet_trade.core.models import Order, OrderStatus, Position, SubPortfolio


@pytest.fixture
def strategy(monkeypatch):
    source = Path(__file__).resolve().parents[4] / "joinquant_strategies/small_cap_enhanced_strategy/yanbin/combined_small_cap_csi300_strategy.py"
    if not source.exists():
        pytest.skip("Combined CSI300 strategy belongs to the parent application")
    tree = ast.parse(source.read_text(encoding="utf-8"))
    methods = {"_order_result", "_order_target_value", "_fill_shortfall",
               "_remaining_buy_budget_under_portion", "rebalance_existing_positions", "phase_buy",
               "_cash_capped_buy_target_value", "_select_buy_candidates",
               "_match_special_code", "_is_black_listed", "_is_white_listed",
               "_sync_snapshot_excluded_positions_option", "_active_positions",
               "_position_market_value", "_managed_portfolio_total_value"}
    functions = {"normalize_special_list_path", "load_special_lists_from_file",
                 "load_snapshot_excluded_positions"}
    definitions = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in functions:
            definitions.append(node)
        if isinstance(node, ast.ClassDef) and node.name in ("CSI300Strategy", "TradeModule"):
            selected = methods if node.name == "CSI300Strategy" else {"_is_order_fully_filled"}
            node.body = [m for m in node.body if isinstance(m, ast.FunctionDef) and m.name in selected]
            definitions.append(node)
    logs = []
    log = SimpleNamespace(**{level: lambda *args, **kwargs: logs.append(args)
                             for level in ("info", "warn", "warning", "error")})
    ns = {"Optional": Optional, "Order": Order, "OrderStatus": OrderStatus, "log": log,
          "LIVE_MODE": True, "ONLINE_ENV": False, "LIVE_DEBUG": False,
          "LIVE_DEBUG_ACTUALLY_TRADE": False, "CSI300_BUY_CASH_BUFFER": .995,
          "g": SimpleNamespace(sync_order_wait_timeout=20), "os": os, "json": json,
          "set_option": settings.set_option, "BLACKLIST_STOCKS": [], "WHITE_LIST_STOCKS": []}
    # Production imports through the parent repository; use the same settings in these tests.
    monkeypatch.setitem(sys.modules, "third_party.bullet_trade.bullet_trade.core.settings", settings)
    exec(compile(ast.Module(body=definitions, type_ignores=[]), str(source), "exec"), ns)
    instance = ns["CSI300Strategy"]()
    instance.pindex = 1
    instance.top_init = 7
    instance.pending_buy_codes = []
    instance.sell_success_count = 0
    state = {"available_cash": None, "pending_value": 0, "pending_buys": [],
             "pending_sells": [], "blocked": False, "cash_ready": True}
    instance._execution_budget = lambda context=None: state
    instance.black_list = []
    instance.white_list = []
    instance._is_kechuang = lambda stock: False
    instance._active_positions = lambda portfolio: portfolio.positions
    instance._active_positions_market_value = lambda portfolio, cd=None: sum(p.value for p in portfolio.positions.values())
    instance._csi300_total_value_portion = lambda: .9
    return instance, ns, state, logs


@pytest.mark.parametrize("status,filled,expected", [
    ("filling", 0, "pending"), ("open", 0, "pending"), ("unknown", 0, "pending"),
    ("filled", 100, "filled"), ("rejected", 0, "failed"),
    ("partly_canceled", 40, "partial"), ("canceled", 0, "failed"),
])
def test_submission_result_distinguishes_pending_and_completion(strategy, status, filled, expected):
    instance, ns, state, logs = strategy
    obj = Order("test", "002916.XSHE", 100, filled=filled, status=status)
    ns["order_target_value"] = lambda *args, **kwargs: obj
    result = instance._order_target_value(None, obj.security, 10000)
    assert result == expected
    if expected == "pending":
        assert not any("下单失败" in str(args) for args in logs)


def test_late_deep_south_fill_still_consumes_shortfall_budget(strategy):
    instance, ns, state, logs = strategy
    portfolio = SimpleNamespace(positions={c: SimpleNamespace(value=15) for c in ("old1", "old2", "old3")})
    context = SimpleNamespace(subportfolios={1: portfolio})
    targets = ["688082.XSHG", "002916.XSHE", "688521.XSHG", "300394.XSHE"]
    instance._target_managed_portfolio_total_value = lambda *args, **kwargs: (90, 100, .9)
    instance._select_buy_candidates = lambda *args, **kwargs: targets
    instance._can_buy_one_lot = lambda *args, **kwargs: True
    instance._portion_capped_buy_target_value = lambda context, code, target, budget, **kwargs: min(target, budget)
    instance._cash_capped_buy_target_value = lambda context, code, target, **kwargs: target
    instance._buy_delta_value = lambda context, code, target, **kwargs: target
    submitted = []
    def submit(context, code, value):
        submitted.append((code, value))
        if code == "002916.XSHE":
            state["pending_buys"].append(code)
            state["pending_value"] += value
            return "pending"
        portfolio.positions[code] = SimpleNamespace(value=value)
        return "filled"
    instance._order_target_value = submit
    instance._fill_shortfall(context, [], set(portfolio.positions), {})
    assert len(submitted) == 4
    assert sum(value for _, value in submitted) == pytest.approx(45)
    assert submitted[-1][1] == pytest.approx(45 - 3 * (90 / 7))
    assert 45 + sum(value for _, value in submitted) <= 90.000001


def test_pending_exposure_counts_towards_portion_limit(strategy):
    instance, ns, state, logs = strategy
    state["pending_value"] = 2000
    portfolio = SimpleNamespace(positions={"one": SimpleNamespace(value=8000)})
    budget, _ = instance._remaining_buy_budget_under_portion(portfolio, 9000)
    assert budget == 0


def test_candidates_exclude_pending_buys_and_sells(strategy):
    instance, ns, state, logs = strategy
    state["pending_buys"] = ["buying"]
    state["pending_sells"] = ["selling"]
    for method in ("_is_black_listed", "_is_paused", "_is_limit_up"):
        setattr(instance, method, lambda *args: False)
    instance._can_buy_one_lot = lambda *args, **kwargs: True
    ranking = [(code, 1) for code in ("held", "buying", "selling", "new")]
    assert instance._select_buy_candidates(None, ranking, 4, 10000, held_codes={"held"}) == ["new"]


def test_buy_phase_counts_pending_names_towards_seven_slots(strategy):
    instance, ns, state, logs = strategy
    state["pending_buys"] = ["new1", "new2", "new3", "new4"]
    context = SimpleNamespace(subportfolios={1: SimpleNamespace(positions={c: object() for c in ("a", "b", "c")})})
    # Returning before ranking lookup proves that pending slots prevent new buys.
    instance.phase_buy(context)
    assert any("跳过买入" in str(args) for args in logs)


def test_topup_waits_for_previous_cash_settlement(strategy):
    instance, ns, state, logs = strategy
    state["cash_ready"] = False
    context = SimpleNamespace(subportfolios={1: SimpleNamespace(positions={"a": object()})})
    instance.rebalance_existing_positions(context)
    assert any("前阶段订单/资金尚未核对完成" in str(args) for args in logs)


def test_each_order_rechecks_portion_and_physical_cash(strategy):
    instance, ns, state, logs = strategy
    portfolio = SimpleNamespace(available_cash=20000, positions={"old": SimpleNamespace(value=8000)})
    context = SimpleNamespace(subportfolios={1: portfolio})
    state["available_cash"] = 5000
    state["pending_value"] = 500
    instance._target_managed_portfolio_total_value = lambda *args, **kwargs: (10000, 11111, .9)
    instance._get_buy_check_price = lambda *args: 10
    instance._get_buy_cash_lock_price = lambda *args, **kwargs: 10
    instance._min_buy_amount = lambda code: 100
    target = instance._cash_capped_buy_target_value(context, "new", 10000)
    assert target == 1500  # 10000 - 8000 actual - 500 pending
    state["pending_value"] = 2000
    assert instance._cash_capped_buy_target_value(context, "new", 10000) is None


@pytest.mark.parametrize("registration", ["initialize", "refresh"])
@pytest.mark.parametrize("legacy_snapshot", [False, True])
def test_blacklisted_holdings_survive_save_restore_and_broker_sync(
    strategy, tmp_path, monkeypatch, registration, legacy_snapshot
):
    from bullet_trade.core import live_runtime
    from bullet_trade.core.live_engine import LiveEngine

    instance, ns, _, _ = strategy
    black_codes = {"002938.XSHE", "002916.XSHE"}
    other_codes = {"300394.XSHE", "600183.XSHG", "688072.XSHG", "688082.XSHG", "688521.XSHG"}
    held_codes = black_codes | other_codes
    white_code = "920180.XBSE"
    special_path = tmp_path / "special_list.json"
    special_path.write_text(json.dumps({"black_list": sorted(black_codes), "white_list": [white_code]}))
    ns["SPECIAL_LIST_PATH"] = str(special_path)
    ns["BLACKLIST_STOCKS"] = ["002916"]  # Prefix entries also must not remove real holdings.
    if registration == "initialize":
        settings.set_option("subportfolio_snapshot_excluded_positions", {
            0: ns["load_snapshot_excluded_positions"](),
            1: ns["load_snapshot_excluded_positions"](),
        })
    else:
        settings.set_option("subportfolio_snapshot_excluded_positions", {1: sorted(black_codes)})
        instance.black_list = sorted(black_codes) + ns["BLACKLIST_STOCKS"]
        instance.white_list = [white_code]
        instance._sync_snapshot_excluded_positions_option()
    settings.set_option("subportfolio_external_cash_sync_ratios", {0: 0.0, 1: 1.0})

    for name in ("_runtime_dir", "_portfolio_ref", "_portfolio_price_refresher",
                 "_execution_state_exporter", "_execution_state_lock"):
        monkeypatch.setattr(live_runtime, name, None)
    live_runtime.init_live_runtime(str(tmp_path))
    engine = LiveEngine(strategy_file=tmp_path / "unused_strategy.py", live_config={
        "runtime_dir": str(tmp_path), "g_autosave_enabled": False,
        "account_sync_enabled": False, "order_sync_enabled": False,
        "tick_sync_enabled": False, "risk_check_enabled": False, "broker_heartbeat_interval": 0,
    })
    positions = {code: Position(security=code, total_amount=100, closeable_amount=100,
                               avg_cost=9, price=10, value=1000)
                 for code in held_codes | {white_code}}
    engine._portfolio.subportfolios = {
        0: SubPortfolio(type="stock", available_cash=0, total_value=0),
        1: SubPortfolio(type="stock", available_cash=100, transferable_cash=100,
                        total_value=8100, positions=positions),
    }
    live_runtime.register_portfolio(engine._portfolio)
    live_runtime.save_subportfolios()
    snapshot_path = tmp_path / "subportfolios.json"
    saved = json.loads(snapshot_path.read_text(encoding="utf-8"))
    assert set(saved["subportfolios"]["1"]["positions"]) == held_codes
    assert saved["subportfolios"]["1"]["total_value"] == pytest.approx(7100)

    if legacy_snapshot:
        sub = saved["subportfolios"]["1"]
        for code in black_codes:
            sub["positions"].pop(code)
        sub["total_value"] = 5100
        snapshot_path.write_text(json.dumps(saved), encoding="utf-8")

    broker_snapshot = {"available_cash": 100, "total_value": 8100, "positions": [
        {"security": code, "amount": 100, "closeable_amount": 100,
         "avg_cost": 9, "current_price": 10, "market_value": 1000}
        for code in held_codes | {white_code}
    ]}
    engine.broker = SimpleNamespace(supports_account_sync=lambda: True,
                                   sync_account=lambda: broker_snapshot, is_connected=lambda: True)
    engine._restore_subportfolios()
    live_runtime.register_portfolio_price_refresher(
        lambda force_log=False: engine._refresh_subportfolio_snapshot_for_save(force_log=force_log)
    )
    live_runtime.save_subportfolios()
    restored = engine._portfolio.subportfolios[1]
    assert set(restored.positions) == held_codes
    assert restored.total_value == pytest.approx(7100)
    saved = json.loads(snapshot_path.read_text(encoding="utf-8"))
    assert set(saved["subportfolios"]["1"]["positions"]) == held_codes
    assert saved["subportfolios"]["1"]["total_value"] == pytest.approx(7100)
    for code in black_codes:
        assert saved["subportfolios"]["1"]["positions"][code]["total_amount"] == 100


def test_blacklist_counts_holdings_and_value_but_blocks_new_buys(strategy, monkeypatch):
    instance, _, _, _ = strategy
    instance.black_list = ["002916"]
    monkeypatch.delattr(instance, "_active_positions")
    portfolio = SubPortfolio(type="stock", available_cash=500, total_value=1500, positions={
        "002916.XSHE": Position(security="002916.XSHE", total_amount=100, price=10, value=1000),
    })
    assert set(instance._active_positions(portfolio)) == {"002916.XSHE"}
    assert instance._managed_portfolio_total_value(portfolio) == 1500
    instance._is_paused = instance._is_limit_up = lambda *args: False
    instance._can_buy_one_lot = lambda *args, **kwargs: True
    assert instance._select_buy_candidates(
        None, [("002916.XSHE", 1), ("300394.XSHE", .5)], 2, 10000
    ) == ["300394.XSHE"]
    assert instance._cash_capped_buy_target_value(None, "002916.XSHE", 10000) is None


@pytest.mark.parametrize("target_value", [0, 500])
def test_blacklist_allows_existing_position_sale(strategy, target_value):
    instance, ns, _, _ = strategy
    instance.black_list = ["002916.XSHE"]
    submitted = []
    def submit(stock, value, **kwargs):
        submitted.append((stock, value))
        return Order("sell", stock, -100, filled=100, status="filled")
    ns["order_target_value"] = submit
    assert instance._order_target_value(None, "002916.XSHE", target_value) == "filled"
    assert submitted == [("002916.XSHE", target_value)]


def test_topup_skips_blacklisted_holding_without_dropping_it(strategy):
    instance, ns, _, _ = strategy
    black_code, normal_code = "002916.XSHE", "300394.XSHE"
    instance.black_list = [black_code]
    instance.top_init = 2
    portfolio = SubPortfolio(type="stock", available_cash=8000, total_value=10000, positions={
        code: Position(security=code, total_amount=100, price=10, value=1000)
        for code in (black_code, normal_code)
    })
    context = SimpleNamespace(subportfolios={1: portfolio})
    ns["get_current_data"] = lambda: {}
    instance._target_managed_portfolio_total_value = lambda *args, **kwargs: (9000, 10000, .9)
    instance._is_paused = instance._is_limit_up = lambda *args: False
    instance._get_buy_check_price = lambda *args: 10
    instance._get_buy_cash_lock_price = lambda *args, **kwargs: 10
    instance._min_buy_amount = lambda code: 100
    instance._format_code_list = lambda codes: str(codes)
    instance._base_alert_lines = lambda context: []
    instance._send_feishu_alert = lambda *args, **kwargs: None
    submitted = []
    def submit(context, stock, value):
        submitted.append((stock, value))
        return "filled"
    instance._order_target_value = submit
    instance.rebalance_existing_positions(context)
    assert submitted == [(normal_code, 4500)]
    assert portfolio.positions[black_code].total_amount == 100
    assert len(portfolio.positions) == 2
