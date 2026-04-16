"""
Live 运行态管理

能力：
- 初始化/加载运行目录并恢复 g
- 周期性持久化 g
- 记录异步调度器游标与 Tick 订阅状态，重启后恢复
"""

from __future__ import annotations

import atexit
import json
import os
import pickle
import threading
import time
from datetime import datetime
from typing import Any, Dict, Optional, Sequence, Set, Tuple

from .globals import g, log


_runtime_dir: Optional[str] = None
_autosave_thread: Optional[threading.Thread] = None
_stop_flag = threading.Event()
_state_cache: Optional[Dict[str, Any]] = None
_state_lock = threading.Lock()
_restored_from_disk = False

# 子账户持久化：由 LiveEngine 注册 portfolio 引用，save_g 时自动保存
_portfolio_ref: Any = None


def _g_path() -> str:
    assert _runtime_dir is not None
    return os.path.join(_runtime_dir, 'g.pkl')


def _state_path() -> str:
    assert _runtime_dir is not None
    return os.path.join(_runtime_dir, 'live_state.json')


def _load_state() -> Dict[str, Any]:
    global _state_cache
    if _runtime_dir is None:
        return {}
    with _state_lock:
        if _state_cache is not None:
            return _state_cache
        path = _state_path()
        try:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    _state_cache = json.load(f)
            else:
                _state_cache = {}
        except Exception:
            _state_cache = {}
        return _state_cache


def _write_state(state: Dict[str, Any]) -> None:
    if _runtime_dir is None:
        return
    with _state_lock:
        tmp = _state_path() + '.tmp'
        os.makedirs(os.path.dirname(tmp), exist_ok=True)
        try:
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)
            os.replace(tmp, _state_path())
            global _state_cache
            _state_cache = dict(state)
        except Exception:
            pass


def init_live_runtime(runtime_dir: str) -> None:
    """
    初始化 live 运行态：创建目录并尝试加载 g，同时准备扩展状态。
    """
    global _runtime_dir, _restored_from_disk
    _runtime_dir = os.path.abspath(os.path.expanduser(runtime_dir))
    os.makedirs(_runtime_dir, exist_ok=True)
    # 重置状态缓存
    global _state_cache
    _state_cache = None
    _restored_from_disk = False
    # 加载 g
    try:
        path = _g_path()
        if os.path.exists(path):
            with open(path, 'rb') as f:
                data = pickle.load(f)
            if isinstance(data, dict):
                # 仅替换内部数据字典
                g._data = data  # type: ignore[attr-defined]
                _restored_from_disk = True
                log.warn(f"** [live_runtime] 成功加载全局变量 g {g}")
        else:
            log.warn(f"init_live_runtime g 路径 {path} 不存在")
    except Exception as e:
        # 读取失败不阻断
        log.error("init_live_runtime 加载 g 失败")
        pass


def save_g() -> None:
    """
    立即保存 g 到 RUNTIME_DIR/g.pkl，并附带保存子账户快照。
    """
    if _runtime_dir is None:
        return
    try:
        tmp = _g_path() + '.tmp'
        with open(tmp, 'wb') as f:
            pickle.dump(getattr(g, '_data', {}), f, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp, _g_path())
        log.info(f'🛟 已保存 g 到 {_g_path()}')

    except Exception as e:
        log.error(f'🛟 保存 g 失败: {e}')

    # 附带保存子账户快照
    try:
        save_subportfolios()
    except Exception:
        pass


def _autosave_worker(interval_sec: int) -> None:
    while not _stop_flag.is_set():
        time.sleep(max(1, interval_sec))
        try:
            save_g()
        except Exception:
            continue


def start_g_autosave(interval_sec: int = 60) -> None:
    """
    启动后台线程周期性保存 g。
    """
    global _autosave_thread
    _stop_flag.clear()
    if _autosave_thread and _autosave_thread.is_alive():
        return
    _autosave_thread = threading.Thread(target=_autosave_worker, args=(interval_sec,), daemon=True)
    _autosave_thread.start()
    atexit.register(save_g)


def stop_g_autosave() -> None:
    _stop_flag.set()
    try:
        if _autosave_thread and _autosave_thread.is_alive():
            _autosave_thread.join(timeout=1)
    except Exception:
        pass


def load_scheduler_cursor() -> Optional[datetime]:
    """
    读取最近一次调度游标（用于重启恢复）。
    """
    state = _load_state()
    cursor = (state.get('scheduler') or {}).get('last_cursor')
    if not cursor:
        return None
    try:
        return datetime.fromisoformat(cursor)
    except Exception:
        return None


def persist_scheduler_cursor(dt: datetime) -> None:
    """
    保存调度游标。
    """
    state = _load_state()
    scheduler = dict(state.get('scheduler') or {})
    scheduler['last_cursor'] = dt.isoformat()
    state['scheduler'] = scheduler
    _write_state(state)


def load_subscription_state() -> Tuple[Set[str], Set[str]]:
    """
    返回上次记录的 tick 订阅（symbol & market）。
    """
    state = _load_state()
    record = state.get('subscriptions') or {}
    symbols = set(record.get('symbols') or [])
    markets = set(record.get('markets') or [])
    return symbols, markets


def persist_subscription_state(symbols: Sequence[str], markets: Sequence[str]) -> None:
    """
    保存 tick 订阅状态，便于重启恢复。
    """
    state = _load_state()
    state['subscriptions'] = {
        'symbols': sorted({str(s) for s in symbols}),
        'markets': sorted({str(m) for m in markets}),
    }
    _write_state(state)


def register_portfolio(portfolio: Any) -> None:
    """注册 Portfolio 引用，使 save_g 自动附带保存子账户状态。"""
    global _portfolio_ref
    _portfolio_ref = portfolio


def _subportfolios_path() -> str:
    assert _runtime_dir is not None
    return os.path.join(_runtime_dir, 'subportfolios.json')


def save_subportfolios() -> None:
    """将所有子账户的 positions + cash 序列化为 JSON。"""
    if _runtime_dir is None or _portfolio_ref is None:
        return
    try:
        # 穿透 LivePortfolioProxy
        backing = getattr(_portfolio_ref, 'backing', _portfolio_ref)
        subs = getattr(backing, 'subportfolios', None)
        if not subs or len(subs) <= 1:
            return

        data: Dict[str, Any] = {'subportfolios': {}, 'saved_at': datetime.now().isoformat()}
        for idx, sp in subs.items():
            positions_data: Dict[str, Any] = {}
            for sec, pos in sp.positions.items():
                positions_data[sec] = {
                    'security': pos.security,
                    'total_amount': pos.total_amount,
                    'closeable_amount': pos.closeable_amount,
                    'avg_cost': pos.avg_cost,
                    'price': pos.price,
                    'acc_avg_cost': getattr(pos, 'acc_avg_cost', pos.avg_cost),
                    'value': pos.value,
                    'side': getattr(pos, 'side', 'long'),
                }
            data['subportfolios'][str(idx)] = {
                'type': sp.type,
                'available_cash': sp.available_cash,
                'transferable_cash': sp.transferable_cash,
                'locked_cash': sp.locked_cash,
                'total_value': sp.total_value,
                'positions': positions_data,
            }

        tmp = _subportfolios_path() + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, _subportfolios_path())
        log.info(f'🛟 已保存子账户快照到 {_subportfolios_path()}')
    except Exception as e:
        log.error(f'🛟 保存子账户快照失败: {e}')


def load_subportfolios() -> Optional[Dict[str, Any]]:
    """读取 subportfolios.json，返回 raw dict 或 None。"""
    if _runtime_dir is None:
        return None
    path = _subportfolios_path()
    try:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        log.warn(f'加载子账户快照失败: {e}')
    return None


def runtime_restored() -> bool:
    return _restored_from_disk


def load_strategy_metadata() -> Dict[str, Any]:
    state = _load_state()
    return dict(state.get('strategy') or {})


def persist_strategy_metadata(metadata: Dict[str, Any]) -> None:
    state = _load_state()
    state['strategy'] = metadata
    _write_state(state)
