"""Compatibility shim for the vendored BulletTrade package.

Why this exists
---------------
In this repo BulletTrade is vendored under:
    third_party/bullet_trade/bullet_trade/

Depending on sys.path / PYTHONPATH, code may import it via either:
    - bullet_trade.core.globals
    - third_party.bullet_trade.bullet_trade.core.globals

Those are *different module names*, and Python can load the same source twice under
different names, which commonly leads to duplicated singletons (e.g. logging
handler setup running twice).

This shim forces the legacy path `third_party.bullet_trade.bullet_trade.*` to
resolve to the canonical `bullet_trade.*` modules by creating `sys.modules`
aliases early.

Preferred solution
------------------
Prefer importing BulletTrade as `bullet_trade.*` and ensure `third_party/bullet_trade`
(or an editable install of it) is on sys.path.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path


def _ensure_vendored_root_on_syspath() -> None:
    vendored_root = Path(__file__).resolve().parent  # .../third_party/bullet_trade
    vendored_root_str = str(vendored_root)
    if vendored_root_str not in sys.path:
        # Prepend so the vendored package wins over any globally installed one.
        sys.path.insert(0, vendored_root_str)


def _alias_bullet_trade_modules() -> None:
    """Alias third_party.bullet_trade.bullet_trade.* -> bullet_trade.*"""
    try:
        _bt = importlib.import_module("bullet_trade")
    except Exception:
        return

    legacy_root = __name__ + ".bullet_trade"  # third_party.bullet_trade.bullet_trade
    sys.modules.setdefault(legacy_root, _bt)

    # Also alias already-imported submodules so mixed imports stay consistent.
    src_prefix = "bullet_trade."
    dst_prefix = legacy_root + "."
    for name, mod in list(sys.modules.items()):
        if name.startswith(src_prefix):
            sys.modules.setdefault(dst_prefix + name[len(src_prefix) :], mod)


_ensure_vendored_root_on_syspath()
_alias_bullet_trade_modules()
