from __future__ import annotations

import logging
import time
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR, localcontext

from .diagnostics import command_context, logged_workflow, workflow_attempt, workflow_retry, workflow_step
from .errors import UiError

LOG = logging.getLogger("fangde")


class OrderAutomation:
    @staticmethod
    def positive_number(value: str, name: str) -> str:
        try:
            parsed = Decimal(str(value))
        except InvalidOperation as exc:
            raise ValueError(f"{name} must be a number: {value}") from exc
        if not parsed.is_finite() or parsed <= 0:
            raise ValueError(f"{name} must be positive: {value}")
        return format(parsed, "f")

    @staticmethod
    def normalize_symbol(value: str) -> str:
        import re
        symbol = value.strip().upper()
        if not re.fullmatch(r"[A-Z0-9.\-]+", symbol):
            raise ValueError(f"Invalid symbol: {value}")
        return symbol

    @staticmethod
    def normalize_side(value: str) -> str:
        side = value.strip().upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"Invalid side: {value}")
        return side.title()

    @classmethod
    def order_price(cls, value: str, side: str, name: str = "price") -> str:
        """Keep cent prices; round finer prices toward the requested order side."""
        requested = cls.positive_number(value, name)
        side = cls.normalize_side(side)
        parsed = Decimal(requested)
        if parsed.as_tuple().exponent >= -2:
            return requested
        with localcontext() as context:
            context.prec = max(context.prec, len(parsed.as_tuple().digits) + 2)
            rounded = parsed.quantize(
                Decimal("0.01"), rounding=ROUND_CEILING if side == "Buy" else ROUND_FLOOR,
            )
        if rounded <= 0:
            raise ValueError(f"{name} rounds down to zero for Sell; use a price of at least 0.01.")
        effective = format(rounded, "f")
        if requested != effective:
            LOG.info(
                "Order price rounded command_id=%s field=%s side=%s requested=%s effective=%s.",
                command_context.get(), name, side, requested, effective,
            )
        return effective
