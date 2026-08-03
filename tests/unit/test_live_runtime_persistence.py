import os

from bullet_trade.core import live_runtime
from bullet_trade.core.globals import g
from bullet_trade.core.live_runtime import (
    init_live_runtime,
    save_g,
    start_g_autosave,
    stop_g_autosave,
)
from bullet_trade.core.models import Portfolio, SubPortfolio


def test_g_persist_cycle(tmp_path):
    runtime = tmp_path / "rt"
    init_live_runtime(str(runtime))
    g.foo = 123
    save_g()

    # 变更并再次初始化，应恢复为持久化的值
    g.foo = 0
    init_live_runtime(str(runtime))
    assert g.foo == 123


def test_autosave_thread(tmp_path):
    runtime = tmp_path / "rt2"
    init_live_runtime(str(runtime))
    g.bar = 456
    start_g_autosave(interval_sec=1)
    # 粗略等待 autosave 执行一次
    import time

    time.sleep(1.5)
    stop_g_autosave()
    path = os.path.join(str(runtime), "g.pkl")
    assert os.path.exists(path)


def test_save_success_logs_immediately_then_at_most_hourly(
    tmp_path,
    monkeypatch,
    caplog,
):
    runtime = tmp_path / "rate_limited_logs"
    clock = [100.0]
    monkeypatch.setattr(live_runtime.time, "monotonic", lambda: clock[0])
    caplog.set_level("INFO", logger="jq_strategy")

    portfolio = Portfolio(
        subportfolios={
            0: SubPortfolio(type="stock", available_cash=1000.0, total_value=1000.0),
            1: SubPortfolio(type="stock", available_cash=2000.0, total_value=2000.0),
        }
    )
    init_live_runtime(str(runtime))
    live_runtime.register_portfolio(portfolio)

    save_g()
    assert "🛟 已保存 g 到" in caplog.text
    assert "🛟 已保存子账户快照到" in caplog.text

    caplog.clear()
    clock[0] += 3599.0
    save_g()
    assert "🛟 已保存 g 到" not in caplog.text
    assert "🛟 已保存子账户快照到" not in caplog.text

    caplog.clear()
    clock[0] += 1.0
    save_g()
    assert "🛟 已保存 g 到" in caplog.text
    assert "🛟 已保存子账户快照到" in caplog.text

    caplog.clear()
    init_live_runtime(str(runtime))
    live_runtime.register_portfolio(portfolio)
    save_g()
    assert "🛟 已保存 g 到" in caplog.text
    assert "🛟 已保存子账户快照到" in caplog.text
