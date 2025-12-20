"""
实盘价格辅助工具：最小价差、价格笼子、保护价计算等。
"""

from __future__ import annotations

import math
from typing import Optional, Tuple, Any


def _split_security(security: str) -> Tuple[str, str]:
    parts = security.split(".")
    if len(parts) == 2:
        return parts[0], parts[1].upper()
    return security, ""


def is_etf(security: str) -> bool:
    code, market = _split_security(security)
    return (market in ("XSHG", "SH") and code.startswith("5")) or (
        market in ("XSHE", "SZ") and code.startswith("1")
    )


def infer_lot_size(security: str) -> int:
    code, market = _split_security(security)
    if market in ("XSHG", "SH", "XSHE", "SZ"):
        # 绝大部分沪深股票/ETF 以 100 股为一手
        return 100
    if market in ("BJ", "BSE"):
        return 100
    # 其他市场（如期货/基金）默认按 1 手
    return 1


def get_min_price_step(security: str, price: float) -> float:
    """
    根据标的和当前价格推断最小价差（tick size）。
    规则参考交易所公开信息，覆盖主板/创业板/ETF/北交所等常见场景。
    """
    code, market = _split_security(security)
    price = float(price) if price and price > 0 else 1.0

    if is_etf(security):
        return 0.001

    # B 股
    if (market in ("XSHG", "SH") and code.startswith("9")) or (
        market in ("XSHE", "SZ") and code.startswith("2")
    ):
        return 0.001

    # 其余沪深 A 股
    if price < 1:
        return 0.001
    return 0.01


def _infer_price_rule(security: str) -> str:
    code, market = _split_security(security)
    if market in ("BJ", "BSE"):
        return "beijing"
    if market in ("XSHG", "SH"):
        if code.startswith("68"):
            return "sci"
        return "main"
    if market in ("XSHE", "SZ"):
        # 创业板（30开头）和主板同一规则（102%/98% + 十档）
        return "main"
    return "other"


def compute_price_bounds(security: str, base_price: float, tick_size: float) -> Tuple[Optional[float], Optional[float]]:
    """
    返回 (买入上限, 卖出下限)，用于价格笼子裁剪。
    base_price 来自交易所“基准价”的近似值（通常取 last_price）。
    """
    rule = _infer_price_rule(security)
    tick = tick_size if tick_size > 0 else 0.01
    if rule == "beijing":
        return (
            max(base_price * 1.05, base_price + 0.1),
            min(base_price * 0.95, base_price - 0.1),
        )
    if rule == "sci":
        return base_price * 1.02, base_price * 0.98
    if rule == "main":
        extra = 10 * tick
        return max(base_price * 1.02, base_price + extra), min(base_price * 0.98, base_price - extra)
    # 其他市场不强制（返回 None）
    return None, None


def _clamp(value: float, lower: Optional[float], upper: Optional[float]) -> float:
    if lower is not None:
        value = max(value, lower)
    if upper is not None:
        value = min(value, upper)
    return value


def _round_to_tick(price: float, tick_size: float, mode: str = "nearest") -> float:
    """将价格对齐到最小价差。

    mode:
      - "nearest": 四舍五入到最近 tick
      - "floor": 向下取整（不高于原价）
      - "ceil": 向上取整（不低于原价）
    """
    if tick_size <= 0:
        return price

    scaled = price / tick_size
    if mode == "floor":
        # +eps 避免由于浮点误差导致 floor 少 1 个 tick
        steps = math.floor(scaled + 1e-12)
    elif mode == "ceil":
        # -eps 避免由于浮点误差导致 ceil 多 1 个 tick
        steps = math.ceil(scaled - 1e-12)
    else:
        steps = round(scaled)

    rounded = steps * tick_size
    return float(f"{rounded:.6f}")


def compute_market_protect_price(
    security: str,
    last_price: float,
    high_limit: Optional[float],
    low_limit: Optional[float],
    percent: float,
    is_buy: bool,
) -> float:
    """
    计算市价保护价：last_price*(1+percent) 经价格笼子/涨跌停/最小价差裁剪。
    """
    base_price = float(last_price)
    if base_price <= 0:
        # 尝试使用涨跌停作为基准
        fallback = high_limit if high_limit and high_limit > 0 else low_limit
        if not fallback:
            raise ValueError(f"{security} 缺少可用价格，无法计算保护价")
        base_price = float(fallback)

    tick = get_min_price_step(security, base_price)
    cage_buy, cage_sell = compute_price_bounds(security, base_price, tick)

    protect_price = base_price * (1.0 + percent)
    current_high = high_limit if high_limit and high_limit > 0 else None
    current_low = low_limit if low_limit and low_limit > 0 else None

    if is_buy:
        protect_price = _clamp(protect_price, current_low, current_high)
        protect_price = _clamp(protect_price, None, cage_buy)
    else:
        protect_price = _clamp(protect_price, cage_sell, current_high)
        protect_price = _clamp(protect_price, current_low, None)

    # 保护价的语义：买入不应被 tick 对齐后抬高；卖出不应被 tick 对齐后压低
    rounded = _round_to_tick(protect_price, tick, mode="floor" if is_buy else "ceil")
    # rounding 可能突破笼子，再次裁剪
    if is_buy:
        rounded = _clamp(rounded, current_low, current_high)
        rounded = _clamp(rounded, None, cage_buy)
    else:
        rounded = _clamp(rounded, cage_sell, current_high)
        rounded = _clamp(rounded, current_low, None)

    if rounded <= 0:
        raise ValueError(f"{security} 保护价无效: {rounded}")
    return rounded


def resolve_market_percent(style: Any, is_buy: bool, default_buy: float, default_sell: float) -> float:
    """
    计算市价单使用的比例：策略 style > 配置 > 默认。
    style 只要带有 buy_price_percent/sell_price_percent 属性即可（鸭子类型）。
    """
    if style and hasattr(style, "buy_price_percent") and hasattr(style, "sell_price_percent"):
        percent = style.buy_price_percent if is_buy else style.sell_price_percent
        if percent is not None:
            return float(percent)
    return float(default_buy if is_buy else default_sell)
