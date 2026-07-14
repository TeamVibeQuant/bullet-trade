from __future__ import annotations

"""
基于账户快照(dict)的打印工具，输出风格对齐策略里的 print_portfolio_info。

输入快照需包含键：
- total_value: float 总资产
- available_cash: float 可用资金
- positions: List[dict]，元素包含：
  - security/code, amount/total_amount, closeable_amount, avg_cost,
    current_price/price/last_price, market_value

此模块不依赖回测上下文，适合在 server / 券商适配层打印概览。
"""

from typing import Any, Dict, List, Sequence
import unicodedata


def render_account_overview(snapshot: Dict[str, Any], limit: int = 888) -> str:
    try:
        positions = list(snapshot.get("positions") or [])
        total_value = _to_float(snapshot.get("total_value"))
        cash = _to_float(snapshot.get("available_cash"))
        invested = 0.0
        entries: List[Dict[str, Any]] = []
        for item in positions:
            code = item.get("security") or item.get("code")
            if not code:
                continue
            amount = int(item.get("amount", item.get("total_amount", 0)) or 0)
            if amount <= 0:
                continue
            closeable = int(item.get("closeable_amount", amount) or amount)
            avg_cost = _to_float(item.get("avg_cost"))
            price = _position_price(item, amount)
            value = _to_float(item.get("market_value"), default=price * amount)
            if value == 0.0:
                value = price * amount
            invested += value
            cost_value = avg_cost * amount
            pnl = value - cost_value
            pnl_pct = (pnl / cost_value * 100.0) if cost_value > 0 else 0.0
            weight = ((value / total_value) * 100.0) if total_value > 0 else 0.0
            name = item.get("display_name") or item.get("name") or ""
            entries.append(
                {
                    "code": code,
                    "name": name,
                    "amount": amount,
                    "closeable": closeable,
                    "avg_cost": avg_cost,
                    "price": price,
                    "value": value,
                    "pnl": pnl,
                    "pnl_pct": pnl_pct,
                    "weight": weight,
                }
            )

        position_ratio = (invested / total_value * 100.0) if total_value > 0 else 0.0
        header = (
            f"📊 券商账户概览: 总资产 {_fmt_currency(total_value)}, 可用资金 {_fmt_currency(cash)}, 仓位 {position_ratio:.2f}%"
        )

        if not entries:
            return header + "\n当前持仓：无"

        entries.sort(key=lambda x: x["value"], reverse=True)
        entries = entries[:limit]
        headers = ["股票代码", "名称", "持仓", "可用", "成本价", "现价", "市值", "盈亏", "盈亏%", "占比%"]
        rows = [
            [
                entry["code"],
                entry["name"],
                str(entry["amount"]),
                str(entry["closeable"]),
                f"{entry['avg_cost']:.3f}",
                f"{entry['price']:.3f}",
                f"{entry['value']:,.2f}",
                f"{entry['pnl']:,.2f}",
                f"{entry['pnl_pct']:.2f}%",
                f"{entry['weight']:.2f}%",
            ]
            for entry in entries
        ]
        return header + "\n" + _render_table(headers, rows)
    except Exception:
        # 出错时回退到简要行
        cash = snapshot.get("available_cash")
        total = snapshot.get("total_value")
        pos_cnt = len(snapshot.get("positions") or [])
        return f"账户概览: 总资产 {total}, 可用 {cash}, 持仓 {pos_cnt}"


def _fmt_currency(value: float) -> str:
    return f"{value:,.2f}"


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _position_price(item: Dict[str, Any], amount: int) -> float:
    raw = item.get("raw") if isinstance(item.get("raw"), dict) else {}
    for candidate in (
        item.get("current_price"),
        item.get("price"),
        item.get("last_price"),
        item.get("lastPrice"),
        item.get("last"),
        item.get("m_dLastPrice"),
        raw.get("m_dLastPrice"),
        raw.get("m_dSettlementPrice"),
        raw.get("m_dLastSettlementPrice"),
    ):
        price = _to_float(candidate)
        if price > 0:
            return price

    market_value = _to_float(item.get("market_value"))
    if market_value <= 0:
        for candidate in (
            item.get("m_dMarketValue"),
            item.get("m_dInstrumentValue"),
            raw.get("m_dMarketValue"),
            raw.get("m_dInstrumentValue"),
        ):
            market_value = _to_float(candidate)
            if market_value > 0:
                break
    if amount > 0 and market_value > 0:
        return market_value / amount
    return 0.0


def _display_width(text: str) -> int:
    width = 0
    for ch in str(text):
        if unicodedata.combining(ch):
            continue
        width += 2 if unicodedata.east_asian_width(ch) in ("F", "W") else 1
    return width


def _pad_cell(text: str, target_width: int) -> str:
    current = _display_width(text)
    padding = max(target_width - current, 0)
    return text + (" " * padding)


def _render_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    widths = [_display_width(h) for h in headers]
    norm_rows: List[List[str]] = []
    for row in rows:
        srow = [str(c) for c in row]
        norm_rows.append(srow)
        for i, cell in enumerate(srow):
            widths[i] = max(widths[i], _display_width(cell))

    def border(char: str) -> str:
        return "+" + "+".join(char * (w + 2) for w in widths) + "+"

    def fmt_row(values: Sequence[str]) -> str:
        segs = [f" {_pad_cell(str(v), widths[i])} " for i, v in enumerate(values)]
        return "|" + "|".join(segs) + "|"

    lines = [border("-"), fmt_row(headers), border("-")]
    for row in norm_rows:
        lines.append(fmt_row(row))
    lines.append(border("-"))
    return "\n".join(lines)


__all__ = ["render_account_overview"]
