from datetime import datetime

import pytest

from bullet_trade.core.live_engine import LiveEngine
from bullet_trade.core.models import Context, Portfolio


def _build_engine() -> LiveEngine:
    engine = LiveEngine.__new__(LiveEngine)
    engine.context = Context(
        portfolio=Portfolio(
            available_cash=0.0,
            transferable_cash=0.0,
            total_value=0.0,
            starting_cash=0.0,
        ),
        current_dt=datetime(2026, 7, 14),
    )
    engine._initial_nav_synced = False
    engine._pending_virtual_orders = {}
    engine._active_order_submissions = 0
    return engine


def test_account_snapshot_and_overview_accept_last_price(caplog):
    engine = _build_engine()
    snapshot = {
        "available_cash": 900.0,
        "total_value": 2000.0,
        "positions": [
            {
                "security": "000001.XSHE",
                "name": "平安银行",
                "amount": 100,
                "closeable_amount": 100,
                "avg_cost": 10.0,
                "last_price": 11.0,
                "market_value": 1100.0,
            }
        ],
    }

    engine._apply_account_snapshot(snapshot)

    position = engine.context.portfolio.positions["000001.XSHE"]
    assert position.price == pytest.approx(11.0)
    assert position.value == pytest.approx(1100.0)

    caplog.set_level("INFO", logger="jq_strategy")
    engine._log_account_positions(snapshot)
    assert "11.000" in caplog.text
    assert "10.00%" in caplog.text
    assert "-100.00%" not in caplog.text


def test_account_snapshot_price_falls_back_to_market_value():
    engine = _build_engine()

    engine._apply_account_snapshot(
        {
            "available_cash": 0.0,
            "total_value": 1100.0,
            "positions": [
                {
                    "security": "000001.XSHE",
                    "amount": 100,
                    "avg_cost": 10.0,
                    "market_value": 1100.0,
                }
            ],
        }
    )

    assert engine.context.portfolio.positions["000001.XSHE"].price == pytest.approx(11.0)


def test_position_snapshot_price_accepts_nested_qmt_raw_fields():
    price = LiveEngine._position_snapshot_price(
        {
            "amount": 100,
            "raw": {
                "m_dLastPrice": 11.2,
                "m_dMarketValue": 1120.0,
            },
        }
    )

    assert price == pytest.approx(11.2)
