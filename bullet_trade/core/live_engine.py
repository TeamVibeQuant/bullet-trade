"""
LiveEngine

异步实盘引擎：
- 结合 AsyncScheduler + EventBus 驱动策略 run_daily/handle_data 钩子
- 感知交易时段并记录延迟，自动跳过午休和收盘后
- 统一券商生命周期、后台任务、tick 订阅和运行态持久化
"""

from __future__ import annotations

import asyncio
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, time as Time, date
import importlib
import hashlib
import inspect
import unicodedata
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Set, Tuple
import pandas as pd

from .async_scheduler import AsyncScheduler, OverlapStrategy
from .event_bus import EventBus
from .events import (
    AfterTradingEndEvent,
    BeforeTradingStartEvent,
    EveryMinuteEvent,
    MarketCloseEvent,
    MarketOpenEvent,
    SystemStartEvent,
    SystemStopEvent,
    TradingDayEndEvent,
    TradingDayStartEvent,
)
from .globals import g, log
from .models import Context, Portfolio, Position, Order, OrderStyle, OrderStatus
from .runtime import set_current_engine
from .scheduler import (
    get_market_periods,
    parse_market_periods_string,
    get_tasks,
    get_trade_calendar,
    run_daily,
    run_weekly,
    run_monthly,
    set_trade_calendar,
    unschedule_all,
)
from . import scheduler as sync_scheduler
from .settings import (
    get_settings,
    set_option,
    reset_settings,
    set_benchmark,
    set_order_cost,
    set_slippage,
    OrderCost,
    FixedSlippage,
    PriceRelatedSlippage,
    StepRelatedSlippage,
)
from ..data.api import get_current_data, set_current_context, get_security_info, get_data_provider
from ..utils.env_loader import (
    get_broker_config,
    get_live_trade_config,
)
from ..broker import BrokerBase, QmtBroker, RemoteQmtBroker
from ..broker.simulator import SimulatorBroker
from .live_runtime import (
    init_live_runtime,
    start_g_autosave,
    stop_g_autosave,
    save_g,
    register_portfolio,
    register_portfolio_price_refresher,
    load_subportfolios,
    load_scheduler_cursor,
    persist_scheduler_cursor,
    load_subscription_state,
    persist_subscription_state,
    runtime_restored,
    load_strategy_metadata,
    persist_strategy_metadata,
)
from .orders import get_order_queue, clear_order_queue, MarketOrderStyle, LimitOrderStyle
from .risk_control import get_global_risk_controller
from .engine import PRE_MARKET_OFFSET, BacktestEngine
from . import pricing


@dataclass
class LiveConfig:
    order_max_volume: int
    trade_max_wait_time: int
    event_time_out: int
    scheduler_market_periods: Optional[str]
    account_sync_interval: int
    account_sync_enabled: bool
    order_sync_interval: int
    order_sync_enabled: bool
    g_autosave_interval: int
    g_autosave_enabled: bool
    tick_subscription_limit: int
    tick_sync_interval: int
    tick_sync_enabled: bool
    risk_check_interval: int
    risk_check_enabled: bool
    broker_heartbeat_interval: int
    runtime_dir: str
    buy_price_percent: float
    sell_price_percent: float
    calendar_skip_weekend: bool = True
    calendar_retry_minutes: int = 20
    portfolio_refresh_throttle_ms: int = 200

    @classmethod
    def load(cls, overrides: Optional[Dict[str, Any]] = None) -> "LiveConfig":
        raw = get_live_trade_config()
        if overrides:
            raw.update(overrides)
        return cls(
            order_max_volume=int(raw.get('order_max_volume', 1_000_000)),
            trade_max_wait_time=int(raw.get('trade_max_wait_time', 16)),
            event_time_out=int(raw.get('event_time_out', 60)),
            scheduler_market_periods=raw.get('scheduler_market_periods'),
            account_sync_interval=int(raw.get('account_sync_interval', 60)),
            account_sync_enabled=bool(raw.get('account_sync_enabled', True)),
            order_sync_interval=int(raw.get('order_sync_interval', 10)),
            order_sync_enabled=bool(raw.get('order_sync_enabled', True)),
            g_autosave_interval=int(raw.get('g_autosave_interval', 60)),
            g_autosave_enabled=bool(raw.get('g_autosave_enabled', True)),
            tick_subscription_limit=int(raw.get('tick_subscription_limit', 100)),
            tick_sync_interval=int(raw.get('tick_sync_interval', 2)),
            tick_sync_enabled=bool(raw.get('tick_sync_enabled', True)),
            risk_check_interval=int(raw.get('risk_check_interval', 300)),
            risk_check_enabled=bool(raw.get('risk_check_enabled', True)),
            broker_heartbeat_interval=int(raw.get('broker_heartbeat_interval', 30)),
            runtime_dir=str(raw.get('runtime_dir', './runtime')),
            buy_price_percent=float(raw.get('market_buy_price_percent', 0.015)),
            sell_price_percent=float(raw.get('market_sell_price_percent', -0.015)),
            portfolio_refresh_throttle_ms=int(raw.get('portfolio_refresh_throttle_ms', 200)),
        )


@dataclass
class _ResolvedOrder:
    security: str
    amount: int
    is_buy: bool
    price: Optional[float]
    last_price: float
    pindex: int
    wait_timeout: Optional[float]
    is_market: bool


class LiveEngine:
    """
    实盘事件引擎。

    - run(): 同步入口，封装 asyncio 事件循环
    - start(): 异步入口，便于测试
    """

    is_live: bool = True

    def __init__(
        self,
        strategy_file: Path | str,
        *,
        broker_name: Optional[str] = None,
        live_config: Optional[Dict[str, Any]] = None,
        broker_factory: Optional[Callable[[], BrokerBase]] = None,
        now_provider: Optional[Callable[[], datetime]] = None,
        sleep_provider: Optional[Callable[[float], Awaitable[None]]] = None,
    ):
        self.strategy_path = Path(strategy_file).resolve()
        self.broker_name = broker_name
        self.config = LiveConfig.load(live_config)
        self._broker_factory = broker_factory
        self._now = now_provider or datetime.now
        self._sleep = sleep_provider

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._background_tasks: List[asyncio.Task] = []

        self.event_bus: Optional[EventBus] = None
        self.async_scheduler: Optional[AsyncScheduler] = None
        self.broker: Optional[BrokerBase] = None

        self._portfolio = Portfolio()
        self.portfolio_proxy = LivePortfolioProxy(self, self._portfolio)
        self.context = Context(portfolio=self.portfolio_proxy, current_dt=self._now())

        self._strategy_loader: Optional[BacktestEngine] = None
        self.initialize_func: Optional[Callable] = None
        self.before_trading_start_func: Optional[Callable] = None
        self.handle_data_func: Optional[Callable] = None
        self.after_trading_end_func: Optional[Callable] = None
        self.process_initialize_func: Optional[Callable] = None
        self.after_code_changed_func: Optional[Callable] = None
        self.handle_tick_func: Optional[Callable] = None

        self._current_day: Optional[date] = None
        self._previous_trade_day: Optional[date] = None
        self._market_periods: List[Tuple[Time, Time]] = []
        self._open_dt: Optional[datetime] = None
        self._close_dt: Optional[datetime] = None
        self._pre_open_dt: Optional[datetime] = None
        self._markers_fired: Set[str] = set()
        self._last_schedule_dt: Optional[datetime] = None
        self._trade_calendar: Dict[date, Dict[str, Any]] = {}
        self._strategy_start_date: Optional[date] = None

        self._tick_symbols: Set[str] = set()
        self._tick_markets: Set[str] = set()
        self._latest_ticks: Dict[str, Dict[str, Any]] = {}
        self._security_name_cache: Dict[str, str] = {}

        self._risk = get_global_risk_controller()
        self._order_lock: Optional[asyncio.Lock] = None
        self._last_account_refresh: Optional[datetime] = None
        self._calendar_guard = TradingCalendarGuard(self.config)
        self._initial_nav_synced: bool = False
        self._provider_tick_callback_bound: bool = False
        self._tick_subscription_updated: bool = False

    # ------------------------------------------------------------------
    # 公共入口
    # ------------------------------------------------------------------

    def run(self) -> int:
        """
        启动 LiveEngine（同步封装）。
        """
        if not self.strategy_path.exists():
            print(f"✗ 策略文件不存在: {self.strategy_path}")
            return 1

        try:
            asyncio.run(self.start())
            return 0
        except KeyboardInterrupt:
            log.info("⚠️  用户终止实盘运行")
            return 0
        except Exception as exc:
            log.error(f"实盘引擎异常退出: {exc}", exc_info=True)
            return 2
        finally:
            try:
                save_g()
            except Exception:
                pass

    async def start(self) -> None:
        """
        异步入口，便于测试复用。
        """
        self._loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()
        self._order_lock = asyncio.Lock()
        self.event_bus = EventBus(self._loop)
        self.async_scheduler = AsyncScheduler()

        await self._bootstrap()

        await self.event_bus.emit(SystemStartEvent())
        try:
            await self._run_loop()
        finally:
            await self.event_bus.emit(SystemStopEvent())
            await self._shutdown()

    # ------------------------------------------------------------------
    # 初始化 & 清理
    # ------------------------------------------------------------------

    async def _bootstrap(self) -> None:
        log.info("🧠 初始化 Live 引擎")
        if not self.strategy_path.exists():
            raise FileNotFoundError(f"策略文件不存在: {self.strategy_path}")

        self._strategy_loader = BacktestEngine(strategy_file=str(self.strategy_path))
        self._strategy_loader.load_strategy()
        self.initialize_func = self._strategy_loader.initialize_func
        self.handle_data_func = self._strategy_loader.handle_data_func
        self.before_trading_start_func = self._strategy_loader.before_trading_start_func
        self.after_trading_end_func = self._strategy_loader.after_trading_end_func
        module = sys.modules.get("strategy")
        if module:
            self.handle_tick_func = getattr(module, 'handle_tick', None)
            if self.process_initialize_func is None:
                self.process_initialize_func = getattr(module, 'process_initialize', None)
            self.after_code_changed_func = getattr(module, 'after_code_changed', None)
        else:
            self.handle_tick_func = None
            self.after_code_changed_func = None

        init_live_runtime(self.config.runtime_dir)
        if self.config.g_autosave_enabled:
            start_g_autosave(self.config.g_autosave_interval)
        g.live_trade = True

        reset_settings()
        set_current_engine(self)
        set_current_context(self.context)
        register_portfolio(self._portfolio)
        register_portfolio_price_refresher(lambda: self._refresh_subportfolio_prices(self._portfolio))
        self.context.run_params['run_type'] = 'LIVE'
        self.context.run_params['is_live'] = True

        current_hash = self._compute_strategy_hash()
        restored_runtime = runtime_restored()
        metadata = load_strategy_metadata()
        metadata_applied = False
        if restored_runtime and metadata:
            metadata_applied = self._restore_strategy_metadata(metadata)
            if not metadata_applied:
                log.warning("检测到历史 g 状态但缺少策略元数据，将重新执行 initialize()")

        log.debug(
            "LiveEngine restore status: restored_runtime=%s, metadata_applied=%s",
            restored_runtime,
            metadata_applied,
        )

        if not restored_runtime or not metadata_applied:
            await self._call_hook(self.initialize_func)

        self._apply_market_period_override()

        hash_changed = bool(metadata) and metadata.get('strategy_hash') and metadata.get('strategy_hash') != current_hash
        if metadata:
            log.debug(
                "LiveEngine: metadata_hash=%s, current_hash=%s, restored=%s",
                metadata.get('strategy_hash'),
                current_hash,
                hash_changed,
            )
        if hash_changed and self.after_code_changed_func:
            await self._call_hook(self.after_code_changed_func)  # NOTE

        self._init_broker()

        await self._call_hook(self.process_initialize_func)

        # 必须在 process_initialize 之后恢复子账户状态，因为
        # process_initialize → initialize() → set_subportfolios 会重建子账户结构。
        # 恢复逻辑：优先从 JSON 快照精确还原，回退按比例分配券商真实资金。
        self._restore_subportfolios()

        self._dedupe_scheduler_tasks()

        # 若策略已通过 run_daily/run_weekly 注册了相同函数，则避免 LiveEngine 再直接调用，防止重复触发
        try:
            tasks = get_tasks()
            if self.before_trading_start_func and any(t.func is self.before_trading_start_func for t in tasks):
                log.debug("LiveEngine: before_market_open 已通过调度注册，跳过直接调用钩子")
                self.before_trading_start_func = None
            if self.handle_data_func and any(t.func is self.handle_data_func for t in tasks):
                log.debug("LiveEngine: market_open/handle_data 已通过调度注册，跳过直接调用钩子")
                self.handle_data_func = None
        except Exception as exc:
            log.warning(f"LiveEngine 调度重复检查失败: {exc}")

        self._migrate_scheduler_tasks()
        self._snapshot_strategy_metadata(current_hash)

        symbols, markets = load_subscription_state()
        self._tick_symbols = set(symbols)
        self._tick_markets = set(markets)
        should_sync_initial = (
            not self._tick_subscription_updated and (self._tick_symbols or self._tick_markets)
        )
        if should_sync_initial:
            self._sync_provider_subscription(initial=True)

        self._last_schedule_dt = load_scheduler_cursor()
        if self._last_schedule_dt:
            current_minute = self._now().replace(second=0, microsecond=0)
            if self._last_schedule_dt > current_minute:
                log.warning(
                    "检测到历史调度游标晚于当前系统时间，已忽略此前的游标值。"
                )
                self._last_schedule_dt = None

        self._start_background_jobs()

    async def _shutdown(self) -> None:
        log.info("🛑 正在关闭 Live 引擎")
        if self._stop_event:
            self._stop_event.set()
        for task in self._background_tasks:
            task.cancel()
        for task in self._background_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                log.warning(f"后台任务退出异常: {exc}")
        self._background_tasks = []

        if self.broker:
            try:
                self.broker.cleanup()
            except Exception as exc:
                log.warning(f"券商清理失败: {exc}")

        if self.config.g_autosave_enabled:
            stop_g_autosave()

    # ------------------------------------------------------------------
    # 主循环
    # ------------------------------------------------------------------

    async def _run_loop(self) -> None:
        assert self._loop is not None
        while not self._stop_event.is_set():
            now = self._now()
            if not await self._calendar_guard.ensure_trade_day(now):
                continue
            await self._ensure_trading_day(now)
            await self._handle_minute_tick(now)
            await self._sleep_until_next_minute(now)

    async def _sleep_until_next_minute(self, now: datetime) -> None:
        target = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        delay = max(0.0, (target - self._now()).total_seconds())
        sleeper = self._sleep or asyncio.sleep
        try:
            await sleeper(delay)
        except asyncio.CancelledError:
            raise

    async def _ensure_trading_day(self, now: datetime) -> None:
        current_date = now.date()
        if self._current_day == current_date:
            return

        self._previous_trade_day = self._current_day
        self._current_day = current_date
        self._markers_fired.clear()

        self._market_periods = get_market_periods()
        if not self._market_periods:
            raise RuntimeError("未配置交易时段，无法运行实盘")
        if self.async_scheduler:
            self.async_scheduler.set_market_periods_resolver(lambda _ref=None: self._market_periods)
        if self._strategy_start_date is None:
            # 如果收盘后启动，start_date 设为下一个交易日
            close_time = self._market_periods[-1][1]  # 收盘时间
            if now.time() > close_time:
                try:
                    provider = get_data_provider('jqdata')
                    trade_days = provider.get_trade_days(start_date=current_date, count=2)
                    if len(trade_days) >= 2:
                        self._strategy_start_date = trade_days[1].date()
                        log.info(f"📅 收盘后启动，start_date 设为下一个交易日 {self._strategy_start_date}")
                except Exception as exc:
                    log.error(f"📅 收盘后启动，start_date 设为下一个交易日, 获取下一个交易日失败: {exc}")
                    self._strategy_start_date = current_date
            else:
                self._strategy_start_date = current_date
            log.info(f"📅 策略开始交易日设为 {self._strategy_start_date}")
        
        try:
            provider = get_data_provider('jqdata')
            calendar_days = provider.get_trade_days(end_date=current_date + timedelta(days=1), count=180)
            calendar_days = calendar_days if len(calendar_days) > 0 else []
            calendar_dates = [pd.to_datetime(d).date() for d in calendar_days]
            if len(calendar_dates) > 0:
                set_trade_calendar(calendar_dates, self._strategy_start_date)
                self._trade_calendar = get_trade_calendar()
                if self.async_scheduler:
                    self.async_scheduler.set_trade_calendar(self._trade_calendar)
        except Exception as exc:
            log.error(f"刷新交易日序号日历失败: {exc}")

        open_dt = datetime.combine(current_date, self._market_periods[0][0])
        close_dt = datetime.combine(current_date, self._market_periods[-1][1])
        self._open_dt = open_dt
        self._close_dt = close_dt
        self._pre_open_dt = open_dt - PRE_MARKET_OFFSET
        try:
            provider = get_data_provider('jqdata')
            previous_days = provider.get_trade_days(end_date=current_date, count=2)  # [previous date, current date]
            for day in previous_days:
                # 统一转换为 date 类型
                if hasattr(day, 'date') and callable(day.date):
                    # datetime, pandas Timestamp 等有 .date() 方法的类型
                    log.debug(f"获取前一个交易日: {day} 类型 {type(day).__name__}，转换为 date")
                    day = day.date()
                elif isinstance(day, date):
                    log.debug(f"获取前一个交易日: {day} 类型 date")
                else:
                    log.debug(f"获取前一个交易日: {day} 类型 {type(day)}")

                if day != current_date:
                    self.context.previous_date = day
                    log.info(f"📅 设置前一个交易日为 {self.context.previous_date}")
                    break
        except Exception as exc:
            log.error(f"获取前一个交易日失败: {exc}")
            self.context.previous_date = self._previous_trade_day
            log.info(f"📅 设置前一个交易日为 {self.context.previous_date}")

        await self.event_bus.emit(TradingDayStartEvent(date=current_date))
        log.info(f"📅 新交易日：{current_date}")

        if self._last_schedule_dt and self._last_schedule_dt.date() != current_date:
            self._last_schedule_dt = None

    async def _handle_minute_tick(self, wall_clock: datetime) -> None:
        current_minute = wall_clock.replace(second=0, microsecond=0)
        if self._last_schedule_dt and self._last_schedule_dt > current_minute:
            log.warning(
                "检测到历史调度游标超前当前时间，将重置为当前分钟之前。"
            )
            self._last_schedule_dt = current_minute - timedelta(minutes=1)
        scheduled = current_minute
        if self._last_schedule_dt and scheduled <= self._last_schedule_dt:
            scheduled = self._last_schedule_dt + timedelta(minutes=1)
        if scheduled > current_minute:
            log.debug(
                "LiveEngine: 已执行至 %s，等待下一触发分钟 %s",
                self._last_schedule_dt,
                scheduled,
            )
            return

        delay = (wall_clock - scheduled).total_seconds()
        timeout = max(0, self.config.event_time_out)

        self.context.previous_dt = self.context.current_dt
        self.context.current_dt = scheduled
        log.set_strategy_time(scheduled)

        if delay > timeout:
            log.warning(
                f"⏱️ 事件超时丢弃: scheduled={scheduled}, delay={delay:.1f}s (> {timeout}s)"
            )
            self._last_schedule_dt = scheduled
            persist_scheduler_cursor(scheduled)
            return

        try:
            if self.async_scheduler:
                await self.async_scheduler.trigger(
                    scheduled,
                    self.context,
                    is_bar=self._is_bar_time(scheduled),
                )
        except Exception as exc:
            log.error(f"异步调度执行失败: {exc}", exc_info=True)

        await self._maybe_emit_market_events(scheduled)
        await self._maybe_handle_data(scheduled)
        await self._process_orders(scheduled)

        self._last_schedule_dt = scheduled
        persist_scheduler_cursor(scheduled)

    def _is_bar_time(self, dt: datetime) -> bool:
        """
        `every_bar` 语义：交易时段内的每一个分钟 bar。
        这里直接复用 `_is_trading_minute`，避免只在开盘分钟触发。
        """
        return self._is_trading_minute(dt)

    def _is_trading_minute(self, dt: datetime) -> bool:
        if dt.second != 0:
            return False
        current = dt.time()
        for start, end in self._market_periods:
            if start <= current < end:
                return True
        return False

    async def _maybe_emit_market_events(self, dt: datetime) -> None:
        assert self.event_bus is not None
        if self._pre_open_dt and 'pre_open' not in self._markers_fired and dt >= self._pre_open_dt:
            self._markers_fired.add('pre_open')
            await self.event_bus.emit(BeforeTradingStartEvent(date=dt.date()))
            await self._call_hook(self.before_trading_start_func)

        if self._open_dt and 'open' not in self._markers_fired and dt >= self._open_dt:
            self._markers_fired.add('open')
            await self.event_bus.emit(MarketOpenEvent(time=dt.strftime("%H:%M:%S")))

        if self._is_trading_minute(dt):
            await self.event_bus.emit(EveryMinuteEvent(time=dt.strftime("%H:%M:%S")))

        if self._close_dt and 'close' not in self._markers_fired and dt >= self._close_dt:
            self._markers_fired.add('close')
            await self.event_bus.emit(MarketCloseEvent(time=dt.strftime("%H:%M:%S")))
            await self._call_hook(self.after_trading_end_func)
            await self.event_bus.emit(AfterTradingEndEvent(date=dt.date()))
            await self.event_bus.emit(TradingDayEndEvent(
                date=dt.date(),
                portfolio_value=self.context.portfolio.total_value,
            ))

    async def _maybe_handle_data(self, dt: datetime) -> None:
        if not self.handle_data_func:
            return
        if not self._is_trading_minute(dt):
            return
        try:
            data = get_current_data()
        except Exception as exc:
            log.warning(f"获取当前数据失败: {exc}")
            data = None
        await self._call_hook(self.handle_data_func, data)

    # ------------------------------------------------------------------
    # 订单处理
    # ------------------------------------------------------------------

    async def _process_orders(self, current_dt: datetime) -> None:
        lock = self._order_lock or asyncio.Lock()
        if self._order_lock is None:
            self._order_lock = lock
        async with lock:
            orders = list(get_order_queue())
            if not orders:
                return
            # 先清空已取出的队列，避免后续处理时误清除新加入的订单
            clear_order_queue()
            if not self.broker:
                log.error("暂无券商实例，无法执行订单")
                return
            try:
                current_data = get_current_data()
            except Exception as exc:
                log.warning(f"获取 current_data 失败，订单无法执行: {exc}")
                return
            open_position_symbols = self._get_open_position_symbols()
            pending_new_positions: Set[str] = set()
            subportfolio_dirty = False
            for order in orders:
                plan = self._build_order_plan(order, current_data)
                if not plan:
                    continue
                try:
                    price_basis = plan.price if plan.price and plan.price > 0 else plan.last_price
                    order_value = float(plan.amount * max(price_basis, 0.0))
                    if order_value <= 0:
                        log.warning(f"订单 {plan.security} 价值异常，忽略执行")
                        continue
                    action = 'buy' if plan.is_buy else 'sell'
                    risk = self._risk
                    if risk:
                        positions_count = len(open_position_symbols | pending_new_positions)
                        total_value = float(getattr(self.context.portfolio, "total_value", 0.0) or 0.0)
                        try:
                            risk.check_order(
                                order_value=order_value,
                                current_positions_count=positions_count,
                                security=plan.security,
                                total_value=total_value,
                                action=action,
                            )
                        except ValueError as risk_exc:
                            log.error(f"风控拒绝委托[{action}] {plan.security}: {risk_exc}")
                            continue
                    price_arg = plan.price if plan.price and plan.price > 0 else None
                    market_flag = bool(plan.is_market)
                    style_obj = getattr(order, "style", None)
                    style_name = style_obj.__class__.__name__ if style_obj else "MarketOrderStyle"
                    price_value = plan.price if plan.price is not None else price_arg
                    price_repr = f"{price_value:.4f}" if price_value else "未指定"
                    price_mode = "市价" if market_flag else "限价"
                    action_label = "买入" if plan.is_buy else "卖出"
                    log.info(
                        f"执行委托[{action_label}] {plan.security}: 行情价={plan.last_price:.4f}, "
                        f"委托价={price_repr}（{price_mode}），风格={style_name}, 数量={plan.amount}"
                    )
                    order_id: Optional[str] = None
                    order.amount = plan.amount
                    order.action = 'open' if plan.is_buy else 'close'
                    order.value = order_value
                    order.price = price_value
                    if plan.is_buy:
                        order_id = await self.broker.buy(
                            plan.security, plan.amount, price_arg, wait_timeout=plan.wait_timeout, market=market_flag
                        )
                    else:
                        order_id = await self.broker.sell(
                            plan.security, plan.amount, price_arg, wait_timeout=plan.wait_timeout, market=market_flag
                        )
                    try:
                        setattr(order, "_broker_order_id", order_id)
                    except Exception:
                        pass
                    log.info(
                        f"委托[{action_label}] {plan.security} 已提交，订单ID={order_id or '未知'}，"
                        f"数量={plan.amount}"
                    )
                    
                    log.info('[LiveEngine] 等待获取订单状态...')
                    status_dict = None
                    try:
                        interval = 0.5  # seconds
                        order_status_timeout = 5  # seconds
                        deadline = time.time() + order_status_timeout
                        while time.time() < deadline:
                            try:
                                status_dict = await self.broker.get_order_status(order_id)
                                status_dict = status_dict or {}
                                st = status_dict.get('status', 'open')
                                if st:
                                    order.status = st
                                    log.info(f"[LiveEngine] 订单状态: {order.status}")
                                    break
                            except Exception as e:
                                log.debug(f"[LiveEngine] 获取订单状态失败 in-while-loop: {e}")
                                pass
                            await asyncio.sleep(interval)
                    except Exception as e:
                        log.debug(f"[LiveEngine] 获取订单状态失败: {e}")
                    
                    if self._apply_virtual_order_fill(order, plan, status_dict):
                        subportfolio_dirty = True

                    if risk:
                        try:
                            risk.record_trade(order_value, action=action)
                        except Exception as record_exc:
                            log.debug(f"记录风控交易失败: {record_exc}")
                        if plan.is_buy and plan.security not in open_position_symbols:
                            pending_new_positions.add(plan.security)
                except Exception as exc:
                    log.error(f"委托失败 {order.security}: {exc}")
            try:
                self.refresh_account_snapshot(force=True)
            except Exception as exc:
                log.debug(f"订单执行后刷新账户快照失败: {exc}")
            if subportfolio_dirty:
                try:
                    save_g()
                except Exception as exc:
                    log.debug(f"订单执行后保存运行态失败: {exc}")

    def _build_order_plan(self, order: Order, current_data) -> Optional[_ResolvedOrder]:
        try:
            snapshot = current_data[order.security]
        except Exception:
            log.warning(f"无法获取 {order.security} 的实时行情，忽略订单")
            return None
        if snapshot.paused:
            log.warning(f"{order.security} 停牌，取消订单")
            return None
        last_price = float(snapshot.last_price or 0.0)
        if last_price <= 0:
            fallback = snapshot.high_limit or snapshot.low_limit
            if not fallback or fallback <= 0:
                log.warning(f"{order.security} 缺少可用价格，忽略订单")
                return None
            last_price = float(fallback)

        amount, is_buy = self._resolve_order_amount(order, last_price)
        if amount <= 0:
            log.debug(f"{order.security} 无需交易或数量不足，跳过")
            return None

        pindex = self._get_order_pindex(order)
        closeable = None
        if not is_buy:
            closeable = self._get_closeable_amount(order.security, pindex=pindex)
            if closeable <= 0:
                log.warning(f"{order.security} 当前无可卖数量，忽略订单")
                return None
        amount = pricing.adjust_order_amount(order.security, amount, is_buy, closeable=closeable)
        if amount <= 0:
            msg = "扣除手数后无可交易数量" if is_buy else "可卖数量不足或不足最小手数"
            log.debug(f"{order.security} {msg}")
            return None

        exec_price: Optional[float] = None
        style_obj = getattr(order, "style", None)
        if isinstance(style_obj, LimitOrderStyle):
            is_market = False
        elif isinstance(style_obj, MarketOrderStyle):
            is_market = style_obj.limit_price is None
        else:
            is_market = True
        if isinstance(style_obj, LimitOrderStyle):
            exec_price = float(style_obj.price)
        elif isinstance(style_obj, MarketOrderStyle) and style_obj.limit_price is not None:
            exec_price = float(style_obj.limit_price)
        else:
            percent = self._resolve_price_percent(style_obj, is_buy)
            try:
                exec_price = pricing.compute_market_protect_price(
                    order.security,
                    snapshot.last_price,
                    getattr(snapshot, "high_limit", None),
                    getattr(snapshot, "low_limit", None),
                    percent,
                    is_buy,
                )
            except Exception as exc:
                log.error(f"{order.security} 无法计算保护价: {exc}")
                return None

        return _ResolvedOrder(
            order.security,
            amount,
            is_buy,
            exec_price,
            last_price,
            pindex,
            getattr(order, "wait_timeout", None),
            is_market,
        )

    def _resolve_order_amount(self, order: Order, last_price: float) -> Tuple[int, bool]:
        price = last_price if last_price > 0 else 1.0
        if getattr(order, "_is_target_amount", False):
            target = int(getattr(order, "_target_amount", 0))
            current = self._get_position_amount(order.security, pindex=self._get_order_pindex(order))
            delta = target - current
            return abs(delta), delta > 0

        if getattr(order, "_is_target_value", False):
            target_value = float(getattr(order, "_target_value", 0.0))
            current_amount = self._get_position_amount(order.security, pindex=self._get_order_pindex(order))
            current_value = current_amount * price
            delta_value = target_value - current_value
            amount = int(abs(delta_value) / price)
            return amount, delta_value > 0

        if hasattr(order, "_target_value") and not getattr(order, "_is_target_value", False):
            target_value = float(getattr(order, "_target_value", 0.0))
            amount = int(abs(target_value) / price)
            return amount, bool(order.is_buy)

        amount = int(order.amount or 0)
        return abs(amount), bool(order.is_buy)

    def _get_order_pindex(self, order: Order) -> int:
        try:
            return int(getattr(order, "pindex", 0) or 0)
        except Exception:
            return 0

    def _portfolio_backing(self) -> Portfolio:
        if isinstance(self.context.portfolio, LivePortfolioProxy):
            return self.portfolio_proxy.backing
        return self.context.portfolio

    def _get_subportfolio(self, pindex: int):
        backing = self._portfolio_backing()
        subs = getattr(backing, "subportfolios", {}) or {}
        if pindex in subs:
            return subs[pindex]
        if pindex == 0:
            return subs.get(0)
        return None

    def _resolve_price_percent(self, style: object, is_buy: bool) -> float:
        return pricing.resolve_market_percent(
            style,
            is_buy,
            self.config.buy_price_percent,
            self.config.sell_price_percent,
        )

    def _get_position_amount(self, security: str, pindex: int = 0) -> int:
        sp = self._get_subportfolio(pindex)
        positions = sp.positions if sp is not None else self._portfolio_backing().positions
        pos = positions.get(security)
        return int(pos.total_amount) if pos else 0

    def _get_open_position_symbols(self) -> Set[str]:
        backing = self._portfolio_backing()
        positions: Dict[str, Position] = {}
        subs = getattr(backing, "subportfolios", {}) or {}
        if subs:
            for sp in subs.values():
                positions.update(getattr(sp, "positions", {}) or {})
        else:
            positions = getattr(backing, "positions", {}) or {}
        result: Set[str] = set()
        for sec, pos in positions.items():
            try:
                amount = int(getattr(pos, "total_amount", 0) or 0)
            except Exception:
                amount = 0
            if amount > 0:
                result.add(sec)
        return result

    def _get_closeable_amount(self, security: str, pindex: int = 0) -> int:
        sp = self._get_subportfolio(pindex)
        positions = sp.positions if sp is not None else self._portfolio_backing().positions
        pos = positions.get(security)
        if not pos:
            return 0
        return int(pos.closeable_amount or pos.total_amount or 0)

    def _apply_virtual_order_fill(
        self,
        order: Order,
        plan: _ResolvedOrder,
        status_snapshot: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Update the strategy-side subportfolio ledger after a live order is accepted.

        Real brokers expose one physical account, while strategies may use multiple
        virtual subportfolios. The broker snapshot cannot tell which pindex owns a
        position, so this ledger must be maintained from the order's pindex.
        """
        status_snapshot = status_snapshot or {}
        raw_status = status_snapshot.get("status", getattr(order, "status", None))
        if isinstance(raw_status, OrderStatus):
            status = raw_status.value
        else:
            status = str(raw_status or "").lower()
        if status in {"rejected", "canceled", "cancelled"}:
            log.info("跳过虚拟子账户更新: %s 状态=%s", plan.security, status)
            return False

        sp = self._get_subportfolio(plan.pindex)
        if sp is None:
            log.warning("无法更新虚拟子账户: pindex=%s 不存在", plan.pindex)
            return False

        amount = int(
            status_snapshot.get("filled_amount")
            or status_snapshot.get("traded_volume")
            or status_snapshot.get("trade_volume")
            or status_snapshot.get("filled_volume")
            or plan.amount
        )
        if amount <= 0:
            return False

        fill_price = self._to_float(
            status_snapshot.get("filled_price")
            or status_snapshot.get("trade_price")
            or status_snapshot.get("price")
            or plan.price
            or plan.last_price,
            default=plan.last_price,
        )
        if fill_price <= 0:
            fill_price = plan.last_price

        if plan.is_buy:
            position = sp.positions.get(plan.security)
            if position is None:
                position = Position(security=plan.security)
                sp.positions[plan.security] = position
            position.update_position(amount, fill_price)
            position.update_price(plan.last_price or fill_price)
            sp.available_cash = max(0.0, float(sp.available_cash or 0.0) - amount * fill_price)
            sp.transferable_cash = min(float(sp.transferable_cash or 0.0), sp.available_cash)
            action = "买入"
        else:
            position = sp.positions.get(plan.security)
            if position is None:
                return False
            sell_amount = min(amount, int(position.total_amount or 0))
            if sell_amount <= 0:
                return False
            position.update_position(-sell_amount, fill_price)
            sp.available_cash = float(sp.available_cash or 0.0) + sell_amount * fill_price
            sp.transferable_cash = float(sp.transferable_cash or 0.0) + sell_amount * fill_price
            if position.total_amount <= 0:
                sp.positions.pop(plan.security, None)
            else:
                position.update_price(plan.last_price or fill_price)
            action = "卖出"

        backing = self._portfolio_backing()
        sp.update_value()
        backing.update_value()
        log.info(
            "虚拟子账户[%s]已更新: %s %s %d股 @ %.4f",
            plan.pindex,
            action,
            plan.security,
            amount,
            fill_price,
        )
        return True

    # ------------------------------------------------------------------
    # 券商管理
    # ------------------------------------------------------------------

    def _create_broker(self) -> BrokerBase:
        if self._broker_factory:
            return self._broker_factory()

        cfg = get_broker_config()
        name = (self.broker_name or cfg.get('default') or 'simulator').lower()

        if name == 'qmt':
            qcfg = cfg.get('qmt') or {}
            account_id = qcfg.get('account_id')
            if not account_id:
                raise RuntimeError("缺少 QMT_ACCOUNT_ID，请在 .env.live 中配置")
            return QmtBroker(
                account_id=account_id,
                account_type=qcfg.get('account_type', 'stock'),
                data_path=qcfg.get('data_path'),
                session_id=qcfg.get('session_id'),
                auto_subscribe=qcfg.get('auto_subscribe'),
            )
        if name == 'qmt-remote':
            rcfg = cfg.get('qmt-remote') or {}
            return RemoteQmtBroker(
                account_id=rcfg.get('account_id') or rcfg.get('account_key') or 'remote',
                account_type=rcfg.get('account_type', 'stock'),
                config=rcfg,
            )
        if name == 'simulator':
            scfg = cfg.get('simulator') or {}
            return SimulatorBroker(
                account_id=scfg.get('account_id', 'simulator'),
                account_type=scfg.get('account_type', 'stock'),
                initial_cash=scfg.get('initial_cash', 1_000_000),
            )
        raise ValueError(f"未知券商类型: {name}")

    # ------------------------------------------------------------------
    # Tick 订阅
    # ------------------------------------------------------------------

    def register_tick_subscription(self, symbols: Sequence[str], markets: Sequence[str]) -> None:
        if not symbols and not markets:
            return
        limit = max(1, self.config.tick_subscription_limit)
        if len(self._tick_symbols.union(symbols)) > limit:
            raise ValueError(f"tick 订阅超限：最多 {limit} 个，当前 {len(self._tick_symbols)} 个")

        self._tick_subscription_updated = True
        self._tick_symbols.update(symbols)
        self._tick_markets.update(markets)
        persist_subscription_state(self._tick_symbols, self._tick_markets)

        self._sync_provider_subscription()
        log.info(
            "已登记 tick 订阅: symbols=%s markets=%s",
            list(self._tick_symbols),
            list(self._tick_markets),
        )

    def unregister_tick_subscription(self, symbols: Sequence[str], markets: Sequence[str]) -> None:
        self._tick_subscription_updated = True
        for sym in symbols:
            self._tick_symbols.discard(sym)
        for mk in markets:
            self._tick_markets.discard(mk)
        persist_subscription_state(self._tick_symbols, self._tick_markets)
        self._sync_provider_subscription(unsubscribe=True, symbols=list(symbols), markets=list(markets))

    def unsubscribe_all_ticks(self) -> None:
        self._tick_subscription_updated = True
        self._tick_symbols.clear()
        self._tick_markets.clear()
        persist_subscription_state(self._tick_symbols, self._tick_markets)
        self._sync_provider_subscription(unsubscribe=True, symbols=None, markets=None)

    def get_current_tick_snapshot(self, symbol: str) -> Optional[Dict[str, Any]]:
        if symbol in self._latest_ticks:
            return self._latest_ticks[symbol]
        tick = self._fetch_tick_snapshot(symbol)
        if tick:
            self._latest_ticks[symbol] = tick
        return tick

    def _fetch_tick_snapshot(self, symbol: str) -> Optional[Dict[str, Any]]:
        try:
            if self.broker and hasattr(self.broker, "get_current_tick"):
                tick = self.broker.get_current_tick(symbol)  # type: ignore[attr-defined]
                if tick:
                    return tick
        except Exception:
            return None
        try:
            provider = get_data_provider()
            if provider and hasattr(provider, "get_current_tick"):
                return provider.get_current_tick(symbol)  # type: ignore[attr-defined]
        except Exception:
            return None
        return None

    def _sync_provider_subscription(
        self,
        initial: bool = False,
        unsubscribe: bool = False,
        symbols: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
    ) -> None:
        """
        将当前订阅状态同步给数据提供者。
        """
        try:
            provider = get_data_provider()
            if not provider:
                return
            if unsubscribe:
                provider.unsubscribe_ticks(symbols)  # type: ignore[attr-defined]
                if markets:
                    provider.unsubscribe_markets(markets)  # type: ignore[attr-defined]
                return
            # subscribe
            if self.handle_tick_func and hasattr(provider, "set_tick_callback"):
                provider.set_tick_callback(self._provider_tick_callback)  # type: ignore[attr-defined]
                self._provider_tick_callback_bound = True
            if self._tick_symbols:
                provider.subscribe_ticks(list(self._tick_symbols))  # type: ignore[attr-defined]
            if self._tick_markets:
                provider.subscribe_markets(list(self._tick_markets))  # type: ignore[attr-defined]
            if initial and (self._tick_symbols or self._tick_markets):
                log.info(
                    "已向数据源同步历史 tick 订阅: symbols=%s markets=%s",
                    list(self._tick_symbols),
                    list(self._tick_markets),
                )
        except Exception as exc:
            log.warning("同步数据源 tick 订阅失败", exc_info=True)

    def _provider_tick_callback(self, data: Any) -> None:
        """
        数据源推送 tick 时的直通回调：不拆分、不加工，直接转发给策略的 handle_tick。
        """
        if not self.handle_tick_func or not self._loop:
            return
        try:
            asyncio.run_coroutine_threadsafe(self._call_hook(self.handle_tick_func, data), self._loop)  # type: ignore[arg-type]
        except Exception:
            pass
    async def _tick_loop(self) -> None:
        if not self.config.tick_sync_enabled:
            return
        interval = max(1, self.config.tick_sync_interval)
        assert self._loop is not None
        while not self._stop_event.is_set():
            # provider 已绑定 tick 回调则不再轮询，避免重复采样或回落到精简快照
            if self._provider_tick_callback_bound:
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    continue
                continue

            if self._tick_symbols:
                for sym in list(self._tick_symbols):
                    try:
                        tick = await self._loop.run_in_executor(None, self._fetch_tick_snapshot, sym)
                    except Exception:
                        tick = None
                    if tick:
                        self._latest_ticks[sym] = tick
                        await self._call_hook(self.handle_tick_func, tick)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue

    # ------------------------------------------------------------------
    # 后台任务
    # ------------------------------------------------------------------

    def _start_background_jobs(self) -> None:
        assert self._loop is not None
        if self.config.account_sync_enabled and self.broker and self.broker.supports_account_sync():
            self._background_tasks.append(self._loop.create_task(
                self._periodic_task(
                    "account-sync",
                    self.config.account_sync_interval,
                    self._account_sync_step,
                )
            ))
        if self.config.order_sync_enabled and self.broker and self.broker.supports_orders_sync():
            self._background_tasks.append(self._loop.create_task(
                self._periodic_task(
                    "order-sync",
                    self.config.order_sync_interval,
                    self._order_sync_step,
                )
            ))
        if self.config.risk_check_enabled:
            self._background_tasks.append(self._loop.create_task(
                self._periodic_task(
                    "risk",
                    self.config.risk_check_interval,
                    self._risk_step,
                )
            ))
        if self.config.broker_heartbeat_interval > 0 and self.broker:
            self._background_tasks.append(self._loop.create_task(
                self._periodic_task(
                    "heartbeat",
                    self.config.broker_heartbeat_interval,
                    self._heartbeat_step,
                )
            ))
        # Tick 轮询
        self._background_tasks.append(self._loop.create_task(self._tick_loop()))

    async def _periodic_task(self, name: str, interval: int, coro_func: Callable[[], Awaitable[None]]) -> None:
        if interval <= 0:
            return
        assert self._loop is not None and self._stop_event is not None
        while not self._stop_event.is_set():
            try:
                await coro_func()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning(f"后台任务 {name} 执行失败: {exc}")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue

    async def _account_sync_step(self) -> None:
        if not self.broker or not self.broker.supports_account_sync():
            return
        assert self._loop is not None
        snapshot = await self._loop.run_in_executor(None, self.broker.sync_account)
        if snapshot:
            try:
                self._apply_account_snapshot(snapshot)
            except Exception as exc:
                log.warning(f"账户同步数据解析失败: {exc}")
            else:
                self._last_account_refresh = datetime.now()

    async def _order_sync_step(self) -> None:
        if not self.broker or not self.broker.supports_orders_sync():
            return
        assert self._loop is not None
        try:
            await self._loop.run_in_executor(None, self.broker.sync_orders)
        except Exception as exc:
            log.debug(f"订单同步失败: {exc}")

    async def _risk_step(self) -> None:
        try:
            summary = self._risk.get_status_summary()
            log.info(summary)
        except Exception:
            pass

    async def _heartbeat_step(self) -> None:
        if not self.broker:
            return
        assert self._loop is not None
        try:
            await self._loop.run_in_executor(None, self.broker.heartbeat)
        except Exception as exc:
            log.warning(f"券商心跳异常: {exc}")

    # ------------------------------------------------------------------
    # 工具函数
    # ------------------------------------------------------------------

    def _restore_subportfolios(self) -> None:
        """券商连接后恢复子账户状态。

        受策略选项 ``use_subportfolio_snapshot`` 控制（默认 True）。
        策略可在 initialize() 中通过 set_option('use_subportfolio_snapshot', False) 关闭。

        A) 快照存在 & 选项为 True（断点恢复）：
           从 subportfolios.json 完整重建子账户结构，保留虚拟子账户归属与数量。
           恢复后会尽量用券商/行情实时价格刷新持仓价格与市值。

        B) 快照不存在（首次启动）：
           initialize() 已调用 set_subportfolios 创建结构，
           用券商真实总资产 starting_cash 按比例缩放现金。
        """
        from .models import SubPortfolio, Position

        settings = get_settings()
        use_snapshot = settings.options.get('use_subportfolio_snapshot', True)

        portfolio = self.context.portfolio
        backing = getattr(portfolio, 'backing', portfolio)

        snapshot = load_subportfolios()
        if use_snapshot and snapshot and 'subportfolios' in snapshot:
            saved_subs = snapshot['subportfolios']
            if len(saved_subs) <= 1:
                log.debug("子账户快照仅含 %d 个子账户，跳过恢复", len(saved_subs))
                return
            saved_at = snapshot.get('saved_at', '未知')
            log.info("从快照恢复子账户状态 (saved_at=%s)", saved_at)

            # 清除现有子账户，从 JSON 完整重建；随后只刷新价格/市值，不改变归属与数量。
            backing.subportfolios.clear()
            for idx_str, saved in saved_subs.items():
                idx = int(idx_str)
                sp = SubPortfolio(
                    type=saved.get('type', 'stock'),
                    available_cash=float(saved['available_cash']),
                    transferable_cash=float(saved.get('transferable_cash', saved['available_cash'])),
                    locked_cash=float(saved.get('locked_cash', 0.0)),
                    total_value=float(saved['total_value']),
                )
                for sec, pos_data in (saved.get('positions') or {}).items():
                    sp.positions[sec] = Position(
                        security=pos_data['security'],
                        total_amount=int(pos_data['total_amount']),
                        closeable_amount=int(pos_data.get('closeable_amount', pos_data['total_amount'])),
                        avg_cost=float(pos_data.get('avg_cost', 0.0)),
                        price=float(pos_data.get('price', 0.0)),
                        acc_avg_cost=float(pos_data.get('acc_avg_cost', pos_data.get('avg_cost', 0.0))),
                        value=float(pos_data.get('value', 0.0)),
                        side=pos_data.get('side', 'long'),
                    )
                backing.subportfolios[idx] = sp
                log.info(
                    "  子账户[%s]: %d 只持仓, 现金 %.2f, 总值 %.2f",
                    idx, len(sp.positions), sp.available_cash, sp.total_value,
                )

            refreshed = self._refresh_subportfolio_prices(backing)
            backing.update_value()
            if refreshed:
                try:
                    save_g()
                except Exception as exc:
                    log.debug(f"恢复子账户后保存刷新价格失败: {exc}")
            return

        # 回退：首次启动（无快照），按比例分配券商真实资金
        subs = backing.subportfolios
        if len(subs) <= 1:
            return

        old_total = sum(sp.total_value for sp in subs.values())
        if old_total <= 0:
            return

        real_total = backing.starting_cash
        if real_total <= 0 or abs(real_total - old_total) < 0.01:
            return

        log.info(
            "首次启动，子账户按比例分配: 原始总额 %.2f → 券商实际 %.2f",
            old_total, real_total,
        )
        for idx, sp in subs.items():
            ratio = sp.total_value / old_total
            old_cash = sp.total_value
            new_cash = real_total * ratio
            sp.available_cash = new_cash
            sp.transferable_cash = new_cash
            sp.total_value = new_cash
            log.info(
                "  子账户[%s]: %.2f → %.2f (%.0f%%)",
                idx, old_cash, new_cash, ratio * 100,
            )

    def _refresh_subportfolio_prices(
        self,
        backing: Portfolio,
        snapshot: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Refresh virtual positions with live broker cost/price without changing ownership."""
        price_map: Dict[str, float] = {}
        avg_cost_map: Dict[str, float] = {}

        if snapshot is None:
            try:
                snapshot = self.broker.sync_account() if self.broker and self.broker.supports_account_sync() else None
            except Exception as exc:
                log.debug(f"刷新子账户持仓信息时获取券商快照失败: {exc}")
                snapshot = None

        for item in (snapshot or {}).get("positions") or []:
            security = item.get("security")
            if not security:
                continue
            avg_cost = self._to_float(item.get("avg_cost"), default=0.0)
            if avg_cost > 0:
                avg_cost_map[security] = avg_cost
            price = self._to_float(item.get("current_price", item.get("price")), default=0.0)
            if price <= 0:
                amount = int(item.get("amount", item.get("total_amount", 0)) or 0)
                market_value = self._to_float(item.get("market_value"), default=0.0)
                if amount > 0 and market_value > 0:
                    price = market_value / amount
            if price > 0:
                price_map[security] = price

        securities: Set[str] = set()
        for sp in (getattr(backing, "subportfolios", {}) or {}).values():
            securities.update((getattr(sp, "positions", {}) or {}).keys())

        for security in securities:
            if security in price_map:
                continue
            tick = self._fetch_tick_snapshot(security)
            if not tick:
                continue
            price = self._to_float(
                tick.get("last_price")
                or tick.get("lastPrice")
                or tick.get("price")
                or tick.get("last"),
                default=0.0,
            )
            if price > 0:
                price_map[security] = price

        if not price_map and not avg_cost_map:
            return False

        refreshed = 0
        for sp in (getattr(backing, "subportfolios", {}) or {}).values():
            for security, pos in (getattr(sp, "positions", {}) or {}).items():
                changed = False
                avg_cost = avg_cost_map.get(security)
                if avg_cost is not None and avg_cost > 0:
                    pos.avg_cost = avg_cost
                    pos.acc_avg_cost = avg_cost
                    changed = True
                price = price_map.get(security)
                if price is not None and price > 0:
                    pos.update_price(price)
                    changed = True
                if changed:
                    refreshed += 1

        if refreshed:
            log.info("刷新子账户持仓成本价/实时价格: %d 条", refreshed)
        return refreshed > 0

    def _init_broker(self) -> None:
        self.broker = self._create_broker()
        self.broker.connect()
        summary = self._safe_account_info()
        positions = summary.get('positions') or []
        log.info(
            "✅ 券商 %s 连接成功: account_id=%s, type=%s, 可用资金=%s, 总资产=%s, 持仓数=%s",
            self.broker.__class__.__name__,
            summary.get('account_id') or getattr(self.broker, 'account_id', ''),
            summary.get('account_type') or getattr(self.broker, 'account_type', ''),
            summary.get('available_cash'),
            summary.get('total_value'),
            len(positions),
        )
        self._log_account_positions(summary)
        if summary:
            self._apply_account_snapshot(summary)

    @staticmethod
    def _task_meta_key(
        module: Optional[str],
        func_name: Optional[str],
        schedule_type: Optional[str],
        time_expr: Any,
        weekday: Any,
        monthday: Any,
    ) -> Tuple[Any, ...]:
        return (
            module or '',
            func_name or '',
            schedule_type or '',
            str(time_expr) if time_expr is not None else '',
            None if weekday is None else int(weekday),
            None if monthday is None else int(monthday),
        )

    def _dedupe_scheduler_tasks(self) -> None:
        tasks = list(get_tasks())
        if not tasks:
            return
        seen = set()
        unique: List[Any] = []
        for task in tasks:
            module = getattr(task.func, '__module__', None)
            name = getattr(task.func, '__name__', None)
            key = self._task_meta_key(
                module,
                name,
                task.schedule_type.value,
                task.time,
                task.weekday,
                task.monthday,
            )
            if key in seen:
                continue
            seen.add(key)
            unique.append(task)
        if len(unique) == len(tasks):
            return
        sync_scheduler._tasks = unique  # type: ignore[attr-defined]

    def _resolve_hook_args(self, func: Callable, extra_args: Tuple[Any, ...]) -> Tuple[Any, ...]:
        base_args: Tuple[Any, ...] = (self.context, *extra_args)
        try:
            sig = inspect.signature(func)
        except (ValueError, TypeError):
            return base_args

        params = list(sig.parameters.values())
        if not params:
            return ()
        if any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in params):
            return base_args

        positional = [
            p
            for p in params
            if p.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]
        max_args = len(positional)
        if max_args <= 0:
            return ()
        if max_args >= len(base_args):
            return base_args
        return base_args[:max_args]

    async def _call_hook(self, func: Optional[Callable], *extra_args) -> None:
        if not func:
            return
        args = self._resolve_hook_args(func, extra_args)
        if asyncio.iscoroutinefunction(func):
            await func(*args)
        else:
            assert self._loop is not None
            await self._loop.run_in_executor(None, lambda: func(*args))

    def _migrate_scheduler_tasks(self) -> None:
        from .scheduler import get_tasks

        tasks = get_tasks()
        if not tasks or not self.async_scheduler:
            return
        for task in tasks:
            strategy = OverlapStrategy.SKIP
            if task.schedule_type.value == 'daily':
                self.async_scheduler.run_daily(task.func, task.time, strategy)
            elif task.schedule_type.value == 'weekly':
                self.async_scheduler.run_weekly(
                    task.func,
                    task.weekday,
                    task.time,
                    task.reference_security,
                    task.force,
                    strategy,
                )
            elif task.schedule_type.value == 'monthly':
                self.async_scheduler.run_monthly(
                    task.func,
                    task.monthday,
                    task.time,
                    task.reference_security,
                    task.force,
                    strategy,
                )

    def _snapshot_strategy_metadata(self, strategy_hash: Optional[str]) -> None:
        try:
            metadata = {
                "version": 1,
                "strategy_hash": strategy_hash,
                "settings": self._collect_settings_snapshot(),
                "tasks": self._collect_scheduler_tasks_snapshot(),
            }
            persist_strategy_metadata(metadata)
        except Exception as exc:
            log.debug(f"策略元数据快照失败: {exc}")

    def _apply_market_period_override(self) -> None:
        expr = (self.config.scheduler_market_periods or "").strip()
        if not expr:
            return
        try:
            periods = parse_market_periods_string(expr)
            set_option('market_period', [(start, end) for start, end in periods])
            log.info("⚙️  已应用自定义交易时段: %s", expr)
        except Exception as exc:
            log.warning("环境变量 SCHEDULER_MARKET_PERIODS 解析失败(%s): %s", expr, exc)

    def _collect_settings_snapshot(self) -> Dict[str, Any]:
        snapshot: Dict[str, Any] = {}
        settings = get_settings()
        snapshot['benchmark'] = settings.benchmark
        options = dict(settings.options or {})
        if isinstance(options.get('market_period'), (list, tuple)):
            options['market_period'] = self._serialize_market_periods(options['market_period'])
        snapshot['options'] = options
        order_cost_snapshot: Dict[str, Dict[str, Any]] = {}
        order_cost_override_snapshot: Dict[str, Dict[str, Any]] = {}
        for asset, cost in (settings.order_cost or {}).items():
            order_cost_snapshot[str(asset)] = {
                'open_tax': cost.open_tax,
                'close_tax': cost.close_tax,
                'open_commission': cost.open_commission,
                'close_commission': cost.close_commission,
                'min_commission': cost.min_commission,
                'close_today_commission': cost.close_today_commission,
                'commission_type': getattr(cost, 'commission_type', 'by_money'),
            }
        snapshot['order_cost'] = order_cost_snapshot
        for asset, cost in (getattr(settings, 'order_cost_overrides', {}) or {}).items():
            order_cost_override_snapshot[str(asset)] = {
                'open_tax': cost.open_tax,
                'close_tax': cost.close_tax,
                'open_commission': cost.open_commission,
                'close_commission': cost.close_commission,
                'min_commission': cost.min_commission,
                'close_today_commission': cost.close_today_commission,
                'commission_type': getattr(cost, 'commission_type', 'by_money'),
            }
        if order_cost_override_snapshot:
            snapshot['order_cost_overrides'] = order_cost_override_snapshot
        if settings.slippage:
            payload = {'class': settings.slippage.__class__.__name__}
            if hasattr(settings.slippage, 'value'):
                payload['value'] = getattr(settings.slippage, 'value', None)
            if hasattr(settings.slippage, 'ratio'):
                payload['ratio'] = getattr(settings.slippage, 'ratio', None)
            if hasattr(settings.slippage, 'steps'):
                payload['steps'] = getattr(settings.slippage, 'steps', None)
            snapshot['slippage'] = payload
        sl_map = getattr(settings, 'slippage_map', {}) or {}
        sl_map_snapshot: Dict[str, Any] = {}
        for key, cfg in sl_map.items():
            payload = self._serialize_slippage_config(cfg)
            if payload is not None:
                sl_map_snapshot[key] = payload
        if sl_map_snapshot:
            snapshot['slippage_map'] = sl_map_snapshot
        return snapshot

    @staticmethod
    def _serialize_slippage_config(config: Any) -> Optional[Dict[str, Any]]:
        if isinstance(config, PriceRelatedSlippage):
            return {'class': 'PriceRelatedSlippage', 'ratio': float(config.ratio)}
        if isinstance(config, StepRelatedSlippage):
            return {'class': 'StepRelatedSlippage', 'steps': int(config.steps)}
        if isinstance(config, FixedSlippage):
            return {'class': 'FixedSlippage', 'value': float(config.value)}
        if hasattr(config, 'to_dict'):
            try:
                return {'class': config.__class__.__name__, **config.to_dict()}
            except Exception:
                return None
        return None

    @staticmethod
    def _deserialize_slippage_config(payload: Dict[str, Any]) -> Optional[Any]:
        if not isinstance(payload, dict):
            return None
        cls = payload.get('class')
        try:
            if cls == 'PriceRelatedSlippage':
                return PriceRelatedSlippage(payload.get('ratio', 0.0))
            if cls == 'StepRelatedSlippage':
                return StepRelatedSlippage(payload.get('steps', 0))
            if cls == 'FixedSlippage':
                return FixedSlippage(payload.get('value', 0.0))
        except Exception:
            return None
        return None

    def _collect_scheduler_tasks_snapshot(self) -> List[Dict[str, Any]]:
        tasks_meta: List[Dict[str, Any]] = []
        seen = set()
        for task in get_tasks():
            func = task.func
            module = getattr(func, '__module__', None)
            name = getattr(func, '__name__', None)
            if not module or not name:
                continue
            key = self._task_meta_key(
                module,
                name,
                task.schedule_type.value,
                task.time,
                task.weekday,
                task.monthday,
            )
            if key in seen:
                continue
            seen.add(key)
            tasks_meta.append(
                {
                    'module': module,
                    'func': name,
                    'schedule_type': task.schedule_type.value,
                    'time': task.time,
                    'weekday': task.weekday,
                    'monthday': task.monthday,
                    'enabled': getattr(task, 'enabled', True),
                }
            )
        return tasks_meta

    def _restore_strategy_metadata(self, meta: Dict[str, Any]) -> bool:
        if not meta or meta.get('version') != 1:
            return False
        try:
            self._apply_settings_snapshot(meta.get('settings') or {})
            self._apply_scheduler_tasks_snapshot(meta.get('tasks') or [])
            return True
        except Exception as exc:
            log.warning(f"恢复策略元数据失败: {exc}")
            return False

    def _apply_settings_snapshot(self, snapshot: Dict[str, Any]) -> None:
        if not snapshot:
            return
        benchmark = snapshot.get('benchmark')
        if benchmark:
            try:
                set_benchmark(benchmark)
            except Exception as exc:
                log.warning(f"恢复 benchmark 失败: {exc}")
        options = snapshot.get('options') or {}
        for key, value in options.items():
            try:
                if key == 'market_period' and value:
                    value = self._deserialize_market_periods(value)
                set_option(key, value)
            except Exception as exc:
                log.debug(f"恢复 option {key} 失败: {exc}")
        order_costs = snapshot.get('order_cost') or {}
        for asset, payload in order_costs.items():
            try:
                cost = OrderCost(**payload)
                set_order_cost(cost, type=asset)
            except Exception as exc:
                log.debug(f"恢复 order_cost({asset}) 失败: {exc}")
        order_cost_overrides = snapshot.get('order_cost_overrides') or {}
        for asset, payload in order_cost_overrides.items():
            try:
                cost = OrderCost(**payload)
                # asset 形如 type_code
                if '_' in asset:
                    type_prefix, ref_code = asset.split('_', 1)
                    set_order_cost(cost, type=type_prefix, ref=ref_code)
            except Exception as exc:
                log.debug(f"恢复 order_cost_overrides({asset}) 失败: {exc}")
        sl_map = snapshot.get('slippage_map') or {}
        if sl_map:
            try:
                settings = get_settings()
                settings.slippage_map = {}
                for key, payload in sl_map.items():
                    cfg = self._deserialize_slippage_config(payload)
                    if cfg:
                        settings.slippage_map[key] = cfg
                if settings.slippage is None and 'all' in settings.slippage_map:
                    settings.slippage = settings.slippage_map.get('all')
            except Exception as exc:
                log.debug(f"恢复 slippage_map 失败: {exc}")
        slippage = snapshot.get('slippage')
        if slippage:
            cls = slippage.get('class')
            try:
                if cls == 'FixedSlippage':
                    set_slippage(FixedSlippage(slippage.get('value', 0.0)))
                elif cls == 'PriceRelatedSlippage':
                    set_slippage(PriceRelatedSlippage(slippage.get('ratio', 0.0)))
                elif cls == 'StepRelatedSlippage':
                    set_slippage(StepRelatedSlippage(slippage.get('steps', 0)))
            except Exception as exc:
                log.debug(f"恢复 slippage 失败: {exc}")

    def _apply_scheduler_tasks_snapshot(self, tasks: List[Dict[str, Any]]) -> None:
        try:
            unschedule_all()
        except Exception:
            pass
        if not tasks:
            return
        normalized_tasks: List[Dict[str, Any]] = []
        seen = set()
        for task_meta in tasks:
            key = self._task_meta_key(
                task_meta.get('module'),
                task_meta.get('func'),
                task_meta.get('schedule_type'),
                task_meta.get('time'),
                task_meta.get('weekday'),
                task_meta.get('monthday'),
            )
            if key in seen:
                continue
            seen.add(key)
            normalized_tasks.append(task_meta)

        for task_meta in normalized_tasks:
            func = self._resolve_callable(task_meta.get('module'), task_meta.get('func'))
            if not func:
                log.warning(f"无法恢复调度任务: {task_meta}")
                continue
            schedule_type = task_meta.get('schedule_type')
            time_expr = task_meta.get('time', 'every_bar')
            enabled = bool(task_meta.get('enabled', True))
            try:
                if schedule_type == 'daily':
                    run_daily(func, time_expr)
                elif schedule_type == 'weekly':
                    run_weekly(func, task_meta.get('weekday'), time_expr)
                elif schedule_type == 'monthly':
                    run_monthly(func, task_meta.get('monthday'), time_expr)
                current_tasks = get_tasks()
                if current_tasks:
                    current_task = current_tasks[-1]
                    current_task.enabled = bool(enabled)
            except Exception as exc:
                log.warning(f"恢复调度任务失败 {task_meta}: {exc}")

    def _resolve_callable(self, module_name: Optional[str], func_name: Optional[str]) -> Optional[Callable]:
        if not module_name or not func_name:
            return None
        module = sys.modules.get(module_name)
        if not module:
            try:
                module = importlib.import_module(module_name)
            except Exception:
                return None
        return getattr(module, func_name, None)

    def _compute_strategy_hash(self) -> Optional[str]:
        try:
            data = self.strategy_path.read_bytes()
            return hashlib.md5(data).hexdigest()
        except Exception:
            return None

    def _serialize_market_periods(self, periods: Sequence[Tuple[Time, Time]]) -> List[List[str]]:
        serialized: List[List[str]] = []
        for start, end in periods:
            if isinstance(start, Time) and isinstance(end, Time):
                serialized.append([start.strftime("%H:%M:%S"), end.strftime("%H:%M:%S")])
        return serialized

    def _deserialize_market_periods(self, raw: Sequence[Sequence[Any]]) -> List[Tuple[Time, Time]]:
        periods: List[Tuple[Time, Time]] = []
        for item in raw:
            if not isinstance(item, (list, tuple)) or len(item) != 2:
                continue
            try:
                start = datetime.strptime(str(item[0]), "%H:%M:%S").time()
                end = datetime.strptime(str(item[1]), "%H:%M:%S").time()
                periods.append((start, end))
            except Exception:
                continue
        return periods

    def refresh_account_snapshot(self, force: bool = False) -> None:
        if not self.broker or not self.broker.supports_account_sync():
            return
        now = datetime.now()
        if not force and self._last_account_refresh and (now - self._last_account_refresh).total_seconds() < 1:
            return
        try:
            snapshot = self.broker.sync_account()
        except Exception as exc:
            log.debug(f"即时账户刷新失败: {exc}")
            return
        if snapshot:
            self._apply_account_snapshot(snapshot)
            self._last_account_refresh = now

    def _apply_account_snapshot(self, snapshot: Dict[str, Any]) -> None:
        try:
            target = self.portfolio_proxy.backing if isinstance(self.context.portfolio, LivePortfolioProxy) else self.context.portfolio
            cash = snapshot.get('available_cash')
            total = snapshot.get('total_value')
            if cash is not None:
                target.available_cash = float(cash)
            if total is not None:
                target.total_value = float(total)
            positions = snapshot.get('positions') or []
            target.positions.clear()
            for item in positions:
                security = item.get('security')
                if not security:
                    continue
                amount = int(item.get('amount', item.get('total_amount', 0)) or 0)
                price = float(item.get('current_price', item.get('price', 0.0)) or 0.0)
                target.positions[security] = Position(
                    security=security,
                    total_amount=amount,
                    closeable_amount=int(item.get('closeable_amount', amount)),
                    avg_cost=float(item.get('avg_cost', 0.0) or 0.0),
                    price=price,
                    value=float(item.get('market_value', amount * price)),
                )

            # 首次连接券商时，用券商真实总资产锚定 starting_cash。
            # 必须在 update_value() 之前设置，因为 update_value 会用子账户
            # 聚合值覆盖 total_value，导致 starting_cash 取到错误的值。
            if not self._initial_nav_synced and total is not None and float(total) > 0:
                target.starting_cash = float(total)
                self._initial_nav_synced = True
                log.info("券商首次同步，starting_cash = %.2f", target.starting_cash)

            # 当只有单个默认子账户时，将券商快照同步到该子账户，
            # 避免 update_value() 聚合时用旧的子账户值覆盖券商数据。
            # 多子账户时不动——各子账户由策略自行管理。
            if len(target.subportfolios) == 1:
                sp = next(iter(target.subportfolios.values()))
                if cash is not None:
                    sp.available_cash = float(cash)
                    sp.transferable_cash = float(cash)
                sp.positions = dict(target.positions)
            else:
                self._refresh_subportfolio_prices(target, snapshot=snapshot)

            target.update_value()
        except Exception as exc:
            log.debug(f"应用账户快照失败: {exc}")
            return

    def _safe_account_info(self) -> Dict[str, Any]:
        if not self.broker:
            return {}
        try:
            info = self.broker.get_account_info() or {}
            # 如果券商返回的是自定义对象，尽量转成 dict
            if not isinstance(info, dict):
                info = getattr(info, '__dict__', {}) or {}
            return info
        except Exception as exc:
            log.debug(f"获取账户信息失败: {exc}")
            return {}

    def _log_account_positions(self, summary: Dict[str, Any], limit: int = 888) -> None:
        """
        以 print_portfolio_info 风格输出券商账户概览，避免原始 list 噪音。
        """
        try:
            positions = list(summary.get('positions') or [])
            total_value = self._to_float(summary.get('total_value'))
            cash = self._to_float(summary.get('available_cash'))
            invested = 0.0
            entries: List[Dict[str, Any]] = []
            for item in positions:
                code = item.get('security') or item.get('code')
                if not code:
                    continue
                amount = int(item.get('amount', item.get('total_amount', 0)) or 0)
                if amount <= 0:
                    continue
                closeable = int(item.get('closeable_amount', amount) or amount)
                avg_cost = self._to_float(item.get('avg_cost'))
                price = self._to_float(item.get('current_price', item.get('price')))
                value = self._to_float(item.get('market_value'), default=price * amount)
                if value == 0.0:
                    value = price * amount
                invested += value
                pnl = value - avg_cost * amount
                pnl_pct = ((price / avg_cost - 1.0) * 100.0) if avg_cost > 0 else 0.0
                weight = ((value / total_value) * 100.0) if total_value > 0 else 0.0
                name = item.get('display_name') or item.get('name') or self._lookup_security_name(code)
                entries.append(
                    {
                        'code': code,
                        'name': name,
                        'amount': amount,
                        'closeable': closeable,
                        'avg_cost': avg_cost,
                        'price': price,
                        'value': value,
                        'pnl': pnl,
                        'pnl_pct': pnl_pct,
                        'weight': weight,
                    }
                )

            position_ratio = (invested / total_value * 100.0) if total_value > 0 else 0.0
            log.info(
                "📊 券商账户概览: 总资产 %s, 可用资金 %s, 仓位 %.2f%%",
                self._format_currency(total_value),
                self._format_currency(cash),
                position_ratio,
            )
            if not entries:
                log.info("当前持仓：无")
                return

            entries.sort(key=lambda x: x['value'], reverse=True)
            entries = entries[:limit]
            headers = ["股票代码", "名称", "持仓", "可用", "成本价", "现价", "市值", "盈亏", "盈亏%", "占比%"]
            rows = [
                [
                    entry['code'],
                    entry['name'],
                    str(entry['amount']),
                    str(entry['closeable']),
                    f"{entry['avg_cost']:.3f}",
                    f"{entry['price']:.3f}",
                    f"{entry['value']:,.2f}",
                    f"{entry['pnl']:,.2f}",
                    f"{entry['pnl_pct']:.2f}%",
                    f"{entry['weight']:.2f}%",
                ]
                for entry in entries
            ]
            log.info("\n" + self._render_table(headers, rows))
        except Exception as exc:
            log.debug(f"打印券商持仓失败: {exc}")

    def _lookup_security_name(self, code: str) -> str:
        if not code:
            return ""
        cached = self._security_name_cache.get(code)
        if cached is not None:
            return cached
        name = ""
        try:
            info = get_security_info(code)
            name = getattr(info, "display_name", None) or getattr(info, "name", "") if info else ""
        except Exception:
            name = ""
        self._security_name_cache[code] = name or ""
        return name or ""

    @classmethod
    def _render_table(cls, headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
        widths = [cls._display_width(str(h)) for h in headers]
        normalized_rows: List[List[str]] = []
        for row in rows:
            str_row = [str(cell) for cell in row]
            normalized_rows.append(str_row)
            for idx, cell in enumerate(str_row):
                widths[idx] = max(widths[idx], cls._display_width(cell))

        def _border(char: str) -> str:
            return "+" + "+".join(char * (w + 2) for w in widths) + "+"

        def _format_row(values: Sequence[str]) -> str:
            segments = [
                f" {cls._pad_cell(str(value), widths[idx])} "
                for idx, value in enumerate(values)
            ]
            return "|" + "|".join(segments) + "|"

        lines = [_border("-"), _format_row(headers), _border("=")]
        for row in normalized_rows:
            lines.append(_format_row(row))
            lines.append(_border("-"))
        return "\n".join(lines)

    @staticmethod
    def _format_currency(value: float) -> str:
        return f"{value:,.2f}"

    @staticmethod
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _display_width(text: str) -> int:
        width = 0
        for char in text:
            if unicodedata.combining(char):
                continue
            east_width = unicodedata.east_asian_width(char)
            width += 2 if east_width in ("F", "W") else 1
        return width

    @classmethod
    def _pad_cell(cls, text: str, target_width: int) -> str:
        current = cls._display_width(text)
        padding = max(target_width - current, 0)
        return text + (" " * padding)
class LivePortfolioProxy:
    """
    代理 Portfolio，确保访问现金/持仓时优先刷新券商快照。
    """

    __slots__ = ("_engine", "_backing", "_last_refresh", "_refresh_interval")

    def __init__(self, engine: "LiveEngine", backing: Portfolio):
        object.__setattr__(self, "_engine", engine)
        object.__setattr__(self, "_backing", backing)
        throttle_ms = getattr(engine.config, "portfolio_refresh_throttle_ms", 200)
        object.__setattr__(self, "_refresh_interval", max(float(throttle_ms) / 1000.0, 0.0))
        object.__setattr__(self, "_last_refresh", datetime.min)

    def _refresh_if_needed(self):
        last = object.__getattribute__(self, "_last_refresh")
        now = datetime.now()
        interval = object.__getattribute__(self, "_refresh_interval")
        if interval > 0 and (now - last).total_seconds() < interval:
            return
        engine = object.__getattribute__(self, "_engine")
        engine.refresh_account_snapshot(force=True)
        object.__setattr__(self, "_last_refresh", now)

    @property
    def available_cash(self) -> float:
        self._refresh_if_needed()
        return object.__getattribute__(self, "_backing").available_cash

    @property
    def total_value(self) -> float:
        self._refresh_if_needed()
        return object.__getattribute__(self, "_backing").total_value

    @property
    def positions(self):
        self._refresh_if_needed()
        return object.__getattribute__(self, "_backing").positions

    def __getattr__(self, item):
        if item.startswith("_"):
            raise AttributeError(item)
        self._refresh_if_needed()
        return getattr(object.__getattribute__(self, "_backing"), item)

    def __setattr__(self, key, value):
        if key in LivePortfolioProxy.__slots__:
            object.__setattr__(self, key, value)
        else:
            setattr(object.__getattribute__(self, "_backing"), key, value)

    @property
    def backing(self) -> Portfolio:
        return object.__getattribute__(self, "_backing")


class TradingCalendarGuard:
    """
    控制交易日启动行为：如果今天不是交易日则等待下次检查。
    """

    def __init__(self, config: LiveConfig):
        self.config = config
        self._next_check: Optional[datetime] = None
        self._confirmed_date: Optional[date] = None

    async def ensure_trade_day(self, now: datetime) -> bool:
        today = now.date()
        if self._confirmed_date == today:
            return True
        if self._next_check and now < self._next_check:
            return False
        if await self._is_trading_day(today):
            self._confirmed_date = today
            return True
        wait_minutes = max(5, int(self._config_value("calendar_retry_minutes", 20)))
        self._next_check = now + timedelta(minutes=wait_minutes)
        log.debug("今日非交易日，下一次检查时间 %s", self._next_check.strftime("%Y-%m-%d %H:%M"))
        return False

    async def _is_trading_day(self, target: date) -> bool:
        try:
            from bullet_trade.data.api import get_trade_days

            days = get_trade_days(str(target), str(target))
            if hasattr(days, "empty"):
                return not days.empty
        except Exception:
            pass
        weekend_skip = bool(self._config_value("calendar_skip_weekend", True))
        if weekend_skip and target.weekday() >= 5:
            return False
        return True

    def _config_value(self, name: str, default: Any) -> Any:
        if hasattr(self.config, name):
            value = getattr(self.config, name)
            if value is not None:
                return value
        if isinstance(self.config, dict):
            value = self.config.get(name)
            if value is not None:
                return value
        return default
