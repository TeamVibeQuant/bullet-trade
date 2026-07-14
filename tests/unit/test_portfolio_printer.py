from bullet_trade.utils.portfolio_printer import render_account_overview


def test_render_account_overview_accepts_last_price():
    output = render_account_overview(
        {
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
    )

    assert "11.000" in output
    assert "10.00%" in output
    assert "-100.00%" not in output


def test_render_account_overview_price_falls_back_to_market_value():
    output = render_account_overview(
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

    assert "11.000" in output
    assert "10.00%" in output
