"""实盘高频调试日志节流测试。"""

import asyncio
import logging
from datetime import datetime

import pytest

from bullet_trade.core import event_bus as event_bus_module
from bullet_trade.core import live_engine as live_engine_module
from bullet_trade.core.event_bus import EventBus
from bullet_trade.core.events import EveryMinuteEvent, MarketOpenEvent
from bullet_trade.core.live_engine import LiveEngine


@pytest.mark.asyncio
async def test_event_bus_no_subscriber_logs_are_rate_limited_by_event_type(
    monkeypatch,
    caplog,
):
    """无订阅者时仍统计每次事件，但同一事件类型的日志每小时最多一条。"""
    clock = [100.0]
    monkeypatch.setattr(event_bus_module, "_monotonic", lambda: clock[0])
    monkeypatch.setattr(logging.getLogger("bullet_trade"), "propagate", True)
    caplog.set_level(logging.DEBUG, logger=event_bus_module.logger.name)
    bus = EventBus(asyncio.get_running_loop())

    await bus.emit(EveryMinuteEvent(time="09:30:00"))
    await bus.emit(EveryMinuteEvent(time="09:31:00"))
    await bus.emit(MarketOpenEvent(time="09:31:00"))

    clock[0] += 3599.0
    await bus.emit(EveryMinuteEvent(time="10:30:00"))
    clock[0] += 1.0
    await bus.emit(EveryMinuteEvent(time="10:31:00"))

    messages = [record.getMessage() for record in caplog.records]
    assert messages.count("📢 事件 EveryMinuteEvent 没有订阅者") == 2
    assert messages.count("📢 事件 MarketOpenEvent 没有订阅者") == 1
    assert bus.get_stats()["events_emitted"] == 5


@pytest.mark.asyncio
async def test_live_engine_wait_log_is_rate_limited_hourly(
    tmp_path,
    monkeypatch,
    caplog,
):
    clock = [100.0]
    monkeypatch.setattr(live_engine_module, "_monotonic", lambda: clock[0])
    caplog.set_level("DEBUG", logger="jq_strategy")
    engine = LiveEngine(
        strategy_file=tmp_path / "strategy.py",
        live_config={"runtime_dir": str(tmp_path / "runtime")},
        now_provider=lambda: datetime(2025, 1, 2, 9, 40, 20),
    )
    engine._last_schedule_dt = datetime(2025, 1, 2, 9, 40)

    await engine._handle_minute_tick(datetime(2025, 1, 2, 9, 40, 20))
    await engine._handle_minute_tick(datetime(2025, 1, 2, 9, 40, 30))
    clock[0] += 3599.0
    await engine._handle_minute_tick(datetime(2025, 1, 2, 9, 40, 40))
    clock[0] += 1.0
    await engine._handle_minute_tick(datetime(2025, 1, 2, 9, 40, 50))

    messages = [
        record.getMessage()
        for record in caplog.records
        if "LiveEngine: 已执行至" in record.getMessage()
    ]
    assert len(messages) == 2
    assert engine._last_schedule_dt == datetime(2025, 1, 2, 9, 40)
