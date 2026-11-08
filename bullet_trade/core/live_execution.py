"""Cash reservations and cumulative fills for physical accounts with virtual portfolios.

This state is saved in the same snapshot as positions/cash. A broker acknowledgement,
an elapsed timeout and a completed fill are deliberately different states.
"""
from __future__ import annotations

import copy
import math
from dataclasses import asdict, dataclass, field, fields
from typing import Any, Dict, Optional


TERMINAL = {"filled", "held", "rejected", "cancelled", "canceled", "partly_canceled"}


def number(value, default=0.0):
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


@dataclass
class ExecutionLedger:
    orders: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    expected_positions: Dict[str, int] = field(default_factory=dict)
    cash_pending: bool = False
    last_fill_at: Optional[str] = None
    broker_cash: Optional[float] = None
    observed_broker_cash: Optional[float] = None
    rejection_cash_limit: Optional[float] = None
    block_reason: str = ""
    fee_allowance: float = 0.0

    def reserve(self, key, plan, created_at, *, cash_price=None):
        if key in self.orders:
            return self.orders[key]
        price = number(cash_price or plan.get("price") or plan["last_price"])
        row = dict(plan)
        row.update(
            local_id=key, broker_id=None, created_at=created_at, status="submitting",
            applied_amount=0, applied_value=0.0, applied_fees=0.0,
            cash_price=price, terminal=False,
        )
        self.orders[key] = row
        return row

    def pending(self, pindex=None):
        return [r for r in self.orders.values() if not r["terminal"]
                and (pindex is None or r["pindex"] == pindex)]

    def reserved_cash(self, pindex=None):
        return sum(self.order_reserve(r) for r in self.pending(pindex))

    @staticmethod
    def order_reserve(row):
        if not row["is_buy"] or row["terminal"]:
            return 0.0
        remaining = max(0, row["amount"] - row["applied_amount"])
        value = remaining * row["cash_price"]
        return value + max(5.0, value * 0.001) if remaining else 0.0

    def pending_value(self, pindex=None):
        return sum(max(0, r["amount"] - r["applied_amount"]) * r["last_price"]
                   for r in self.pending(pindex) if r["is_buy"])

    def update(self, key, snapshot, *, max_delta=None):
        """Return only newly confirmed quantity, notional and fees; never replay a fill."""
        row = self.orders[key]
        raw_status = snapshot.get("status", row["status"])
        status = str(getattr(raw_status, "value", raw_status) or "unknown").lower()
        quantity = None
        for field in ("filled_amount", "filled", "traded_volume", "trade_volume", "filled_volume"):
            if snapshot.get(field) is not None:
                quantity = max(0, int(number(snapshot[field])))
                break
        if quantity is None:
            if status in {"cancelled", "canceled", "partly_canceled"}:
                row["status"] = "settlement_pending"
                return 0, 0.0, 0.0
            quantity = row["amount"] if status in {"filled", "held"} else row["applied_amount"]
        quantity = min(quantity, row["amount"])
        # Out-of-order reports cannot undo a fill or reopen a completed order.
        if quantity < row["applied_amount"] or row["terminal"]:
            return 0, 0.0, 0.0
        delta = quantity - row["applied_amount"]
        if max_delta is not None and delta > max_delta:
            row["status"] = "settlement_pending"
            row["accounting_error"] = "sell_position_mismatch"
            return 0, 0.0, 0.0
        row.pop("accounting_error", None)
        value = row["applied_value"]
        if delta:
            price = next((number(snapshot.get(f)) for f in
                          ("filled_price", "trade_price", "traded_price", "price") if number(snapshot.get(f)) > 0), 0.0)
            if price <= 0:
                row["status"] = "settlement_pending"
                return 0, 0.0, 0.0
            value = quantity * price
            if value <= row["applied_value"]:
                row["status"] = "settlement_pending"
                return 0, 0.0, 0.0
        fees = max(row["applied_fees"], sum(number(snapshot.get(f)) for f in ("commission", "tax")))
        value_delta = value - row["applied_value"]
        fees_delta = fees - row["applied_fees"]
        row.update(status=status, applied_amount=quantity, applied_value=value, applied_fees=fees)
        row["terminal"] = status in TERMINAL
        # A filled status with an incomplete quantity must still be reconciled.
        if status in {"filled", "held"} and quantity < row["amount"]:
            row["terminal"] = False
            row["status"] = "settlement_pending"
        if delta or fees_delta:
            self.cash_pending = True
            self.fee_allowance += value_delta * 0.003 + 5.0
        return delta, value_delta, fees_delta

    def check_cash(self, snapshot, virtual_cash):
        """Validate economics, not time since the last fill or number of polls.

        available_cash excludes broker freezes. With no local pending orders, the
        sum of available and frozen cash can be reconciled to the virtual ledger.
        Unexplained changes need a complete, internally consistent account view.
        """
        cash = number(snapshot.get("available_cash"), None)
        if cash is None or cash < 0:
            self.block_reason = "missing_cash"
            return False
        self.observed_broker_cash = cash
        if self.rejection_cash_limit is not None and cash > self.rejection_cash_limit + 1.0:
            self.block_reason = "broker_cash_rejected"
            return False
        if self.pending():
            self.block_reason = "pending_orders"
            return False
        positions = snapshot.get("positions")
        quantities = {}
        market_value = 0.0
        if isinstance(positions, list):
            for pos in positions:
                security = pos.get("security")
                amount = int(number(pos.get("amount", pos.get("total_amount"))))
                quantities[security] = quantities.get(security, 0) + amount
                value = number(pos.get("market_value", pos.get("value")))
                if value <= 0:
                    value = amount * number(pos.get("current_price", pos.get("price")))
                market_value += value
        if self.cash_pending and any(quantities.get(s, 0) != q for s, q in self.expected_positions.items()):
            self.block_reason = "positions_before_fill"
            return False
        frozen = number(snapshot.get("locked_cash", snapshot.get("frozen_cash")))
        delta = cash + frozen - virtual_cash
        total = number(snapshot.get("total_value"), None)
        tolerance = max(5.0, abs(total or 0) * 0.0001)
        consistent = (isinstance(positions, list) and total is not None
                      and abs(total - cash - frozen - market_value) <= tolerance)
        # A small fee debit is allowed only after quantities have caught up.
        fee_debit = self.cash_pending and -max(5.0, self.fee_allowance) <= delta <= 0
        if abs(delta) > 1.0 and not consistent and not fee_debit:
            self.block_reason = "cash_nav_mismatch"
            return False
        self.cash_pending = False
        self.broker_cash = cash
        self.rejection_cash_limit = None
        self.expected_positions.clear()
        self.fee_allowance = 0.0
        self.block_reason = ""
        return True

    def export(self):
        return {"version": 1, **asdict(self)}

    def restore(self, state):
        if not state:
            return
        if state.get("version") != 1:
            raise ValueError("Unsupported execution ledger snapshot version")
        for item in fields(self):
            if item.name in state:
                setattr(self, item.name, copy.deepcopy(state[item.name]))
