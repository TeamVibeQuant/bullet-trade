import pytest

from bullet_trade.core import pricing


@pytest.mark.unit
def test_a_share_buy_rounding():
    min_lot, step = pricing.infer_lot_rule("000001.XSHE")
    assert min_lot == 100
    assert step == 100
    assert pricing.adjust_order_amount("000001.XSHE", 150, True) == 100
    assert pricing.adjust_order_amount("000001.XSHE", 99, True) == 0


@pytest.mark.unit
def test_sci_board_step_one():
    min_lot, step = pricing.infer_lot_rule("688001.XSHG")
    assert min_lot == 200
    assert step == 1
    assert pricing.adjust_order_amount("688001.XSHG", 201, True) == 201
    assert pricing.adjust_order_amount("688001.XSHG", 199, True) == 0


@pytest.mark.unit
def test_convertible_bond_step_ten():
    min_lot, step = pricing.infer_lot_rule("113000.XSHG")
    assert min_lot == 10
    assert step == 10
    assert pricing.adjust_order_amount("113000.XSHG", 15, True) == 10
    assert pricing.adjust_order_amount("113000.XSHG", 9, True) == 0


@pytest.mark.unit
def test_beijing_board_suffix_compat():
    min_lot_bj, step_bj = pricing.infer_lot_rule("430001.BJ")
    min_lot_bse, step_bse = pricing.infer_lot_rule("430001.BSE")
    assert (min_lot_bj, step_bj) == (min_lot_bse, step_bse)
    assert pricing.adjust_order_amount("430001.BSE", 101, True) == 101


@pytest.mark.unit
def test_sell_odd_lot_allowed():
    assert pricing.adjust_order_amount("000001.XSHE", 20, False, closeable=20) == 20
    assert pricing.adjust_order_amount("000001.XSHE", 50, False, closeable=20) == 20
    assert pricing.adjust_order_amount("000001.XSHE", 150, False, closeable=300) == 100
    assert pricing.adjust_order_amount("300502.XSHE", 240, False, closeable=240) == 240
    assert pricing.adjust_order_amount("300502.XSHE", 50, False, closeable=240) == 0
