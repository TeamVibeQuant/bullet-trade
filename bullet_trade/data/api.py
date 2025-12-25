"""
数据API包装

包装多数据源的函数，避免未来函数，确保回测准确性
"""

from typing import Union, List, Optional, Dict, Any
from datetime import datetime, timedelta, date as Date, time as Time
import pandas as pd
import re
import os
import json


from ..core.models import SecurityUnitData
from ..core.globals import log
from ..core.exceptions import FutureDataError, UserError
from ..core.settings import get_settings
from ..utils.env_loader import get_data_provider_config


# 全局上下文，用于获取当前回测时间
_current_context = None

# 引入可插拔数据提供者，并设置默认Provider
from .providers.base import DataProvider

# 记录是否已在实盘强制关缓存并提醒过
_cache_forced_off_warned = False


def _create_provider(provider_name: Optional[str] = None, overrides: Optional[Dict[str, Any]] = None) -> DataProvider:
    """
    根据名称创建数据提供者实例，支持读取环境配置并按需覆盖参数。
    """
    config = get_data_provider_config()
    target = (provider_name or config.get('default') or 'jqdata').lower()
    overrides = overrides or {}

    if target == 'jqdata':
        from .providers.jqdata import JQDataProvider
        provider_cfg = dict(config.get('jqdata', {}) or {})
        provider_cfg.update(overrides)
        return JQDataProvider(provider_cfg)
    if target in ('tushare',):
        from .providers.tushare import TushareProvider
        provider_cfg = dict(config.get('tushare', {}) or {})
        provider_cfg.update(overrides)
        return TushareProvider(provider_cfg)
    if target in ('qmt', 'miniqmt'):
        from .providers.miniqmt import MiniQMTProvider
        provider_cfg = dict(config.get('qmt', {}) or {})
        provider_cfg.update(overrides)
        return MiniQMTProvider(provider_cfg)
    if target in ('qmt-remote', 'remote-qmt', 'remote_qmt'):
        from .providers.remote_qmt import RemoteQmtProvider
        provider_cfg = dict(config.get('remote_qmt', {}) or {})
        provider_cfg.update(overrides)
        return RemoteQmtProvider(provider_cfg)

    raise ValueError(f"未知的数据提供者: {provider_name}")


_provider_router = dict(
    jqdata=_create_provider('jqdata'),
    qmt=_create_provider('qmt'),
)


_provider: DataProvider = _create_provider()
_auth_attempted = False
_security_info_cache: Dict[str, "SecurityInfo"] = {}
_security_overrides_loaded = False
_security_overrides: Dict[str, Any] = {}


class SecurityInfo(dict):
    """兼容聚宽风格的证券信息对象，既支持属性访问也保留字典语义。"""

    __slots__ = ("code",)

    def __init__(self, code: str, data: Optional[Dict[str, Any]] = None):
        super().__init__()
        object.__setattr__(self, "code", code)
        if data:
            for key, value in data.items():
                if value is not None:
                    super().__setitem__(key, value)

        # 为常用字段设置默认值，避免属性访问时抛异常
        for key in ("display_name", "name", "start_date", "end_date", "type", "subtype", "parent"):
            self.setdefault(key, None)

    def __getattr__(self, item: str) -> Any:
        # 未提供的字段返回 None，贴近聚宽 SDK 的容错行为
        return self.get(item, None)

    def __setattr__(self, key: str, value: Any) -> None:
        if key == "code":
            object.__setattr__(self, key, value)
        else:
            self[key] = value

    def __delattr__(self, item: str) -> None:
        if item == "code":
            raise AttributeError("code 字段不可删除")
        try:
            del self[item]
        except KeyError as exc:
            raise AttributeError(item) from exc

    def to_dict(self) -> Dict[str, Any]:
        return dict(self)

def _ensure_auth():
    """确保数据提供者已认证"""
    global _auth_attempted
    if not _auth_attempted:
        try:
            _provider.auth()
            _auth_attempted = True
        except Exception as e:
            # 认证失败，但不设置 _auth_attempted = True
            # 这样下次调用时还会重试
            print(f"数据源认证失败: {e}")
            # 不设置 _auth_attempted，允许重试

def set_data_provider(provider: Union[DataProvider, str], **provider_kwargs) -> None:
    """
    设置当前数据提供者。
    支持直接传入 DataProvider 实例，或传入 provider 名称（如 'jqdata'、'tushare'、'miniqmt'）。
    """
    global _provider, _auth_attempted, _security_info_cache, _cache_forced_off_warned
    if isinstance(provider, DataProvider):
        _provider = provider
    else:
        _provider = _create_provider(provider_name=provider, overrides=provider_kwargs)

    _auth_attempted = False
    _security_info_cache = {}
    _cache_forced_off_warned = False
    try:
        _provider.auth()
        _auth_attempted = True
    except Exception:
        _auth_attempted = True
        pass

def get_data_provider() -> DataProvider:
    """获取当前数据提供者（若未认证则触发一次认证）"""
    _maybe_disable_cache_for_live()
    _ensure_auth()
    return _provider


def set_current_context(context):
    """设置当前回测上下文"""
    global _current_context
    _current_context = context


def _is_live_mode() -> bool:
    try:
        return bool(_current_context and getattr(_current_context, "run_params", {}).get("is_live"))
    except Exception:
        return False


def _maybe_disable_cache_for_live() -> None:
    """
    实盘模式下强制关闭数据提供者的磁盘缓存，避免延迟/IO风险。
    """
    global _cache_forced_off_warned
    if not _is_live_mode():
        return
    cache_obj = getattr(_provider, "_cache", None)
    if cache_obj is None or not getattr(cache_obj, "enabled", False):
        return
    cache_dir = getattr(cache_obj, "cache_dir", "") or "未配置"
    cache_obj.enabled = False
    if not _cache_forced_off_warned:
        log.warning("实盘模式已强制关闭数据源缓存，原缓存目录: %s", cache_dir)
        _cache_forced_off_warned = True


def _config_base_dir() -> str:
    # bullet_trade/data -> base at bullet_trade
    return os.path.dirname(os.path.dirname(__file__))


def _security_overrides_path() -> str:
    return os.path.join(_config_base_dir(), 'config', 'security_overrides.json')


def _load_security_overrides_if_needed() -> None:
    global _security_overrides_loaded, _security_overrides
    if _security_overrides_loaded:
        return
    path = _security_overrides_path()
    try:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    _security_overrides = data
    except Exception as exc:
        log.debug(f"读取security_overrides失败: {exc}")
        _security_overrides = {}
    finally:
        _security_overrides_loaded = True


def set_security_overrides(overrides: Dict[str, Any]) -> None:
    """以编程方式设置/覆盖标的元数据（category/tplus/slippage）。"""
    global _security_overrides_loaded, _security_overrides
    _security_overrides = overrides or {}
    _security_overrides_loaded = True
    _security_info_cache.clear()


def reset_security_overrides() -> None:
    """清除覆盖并恢复到默认配置文件内容。"""
    global _security_overrides_loaded, _security_overrides
    _security_overrides = {}
    _security_overrides_loaded = False
    _security_info_cache.clear()


def _merge_overrides(security: str, base_info: Dict[str, Any]) -> Dict[str, Any]:
    """将配置覆盖项合并到基础元信息中。支持分类默认与按代码覆盖。"""
    _load_security_overrides_if_needed()
    if not _security_overrides:
        return base_info

    by_code = _security_overrides.get('by_code') or {}
    by_category = _security_overrides.get('by_category') or {}
    by_prefix = _security_overrides.get('by_prefix') or {}

    out = dict(base_info)

    # 推断分类
    category = out.get('category')
    subtype = str(out.get('subtype') or '').lower()
    primary = str(out.get('type') or '').lower()
    if not category:
        if subtype in ('mmf', 'money_market_fund'):
            category = 'money_market_fund'
        elif primary in ('fund', 'etf'):
            # jqdatasdk 返回 type='etf'，统一归类为 fund
            category = 'fund'
        elif primary == 'stock':
            category = 'stock'
        else:
            category = 'stock'
        out['category'] = category

    # 前缀归类（仅在未显式设置 category 或仍为 stock 的情况下尝试）
    try:
        if isinstance(by_prefix, dict):
            code = security.split('.', 1)[0]
            for prefix, cat in by_prefix.items():
                if code.startswith(prefix):
                    # 仅在未显式分类或默认为stock时采用前缀分类
                    if out.get('category') in (None, '', 'stock'):
                        out['category'] = cat
                    break
    except Exception:
        pass

    # 分类默认
    cat_defaults = by_category.get(category, {}) if isinstance(by_category, dict) else {}
    if isinstance(cat_defaults, dict):
        for k, v in cat_defaults.items():
            out.setdefault(k, v)

    # 代码覆盖
    code_over = by_code.get(security, {}) if isinstance(by_code, dict) else {}
    if isinstance(code_over, dict):
        out.update({k: v for k, v in code_over.items() if v is not None})

    return out


class BacktestCurrentData:
    """当前行情（回测/非 tick 路径）。延迟加载，按 (security, time) 缓存。"""

    def __init__(self, context):
        self._context = context
        self._cache: Dict[Any, SecurityUnitData] = {}

    def __getitem__(self, security: str) -> SecurityUnitData:
        current_dt = self._context.current_dt
        cache_key = (security, current_dt)
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            def _decide_window(ts: Union[datetime, Date]) -> Dict[str, Any]:
                ct: Optional[Time] = None
                cd: Optional[Date] = None
                if isinstance(ts, datetime):
                    ct = ts.time()
                    cd = ts.date()
                elif isinstance(ts, Date):
                    cd = ts
                m_start = Time(9, 31)
                m_end = Time(15, 0)
                pre_open = Time(9, 25)
                use_min = bool(ct is not None and m_start <= ct < m_end)
                use_open = bool(ct is not None and pre_open <= ct < m_start)
                return {"use_minute": use_min, "use_open_window": use_open, "current_date": cd}

            win = _decide_window(current_dt)
            use_minute = win["use_minute"]
            use_open_price_window = win["use_open_window"]
            current_date = win["current_date"]

            use_real_price = _get_setting('use_real_price')
            fields = ['open', 'close', 'high_limit', 'low_limit', 'paused']

            def _build_fetch_kwargs(freq_text: str) -> Dict[str, Any]:
                kw = dict(
                    security=security,
                    end_date=current_dt,
                    frequency=freq_text,
                    fields=fields,
                    count=1,
                    fq='pre',
                )
                if use_real_price:
                    pre_ref = current_date or current_dt
                    kw.update(prefer_engine=True, pre_factor_ref_date=pre_ref)
                return kw

            if use_minute:
                df = _provider.get_price(**_build_fetch_kwargs('minute'))
            else:
                df = _provider.get_price(**_build_fetch_kwargs('daily'))

            if not df.empty:
                if 'time' in df.columns and 'code' in df.columns:
                    row = df.iloc[-1]
                    close_price = float(row['close']) if pd.notna(row['close']) else 0.0
                    high_limit = float(row.get('high_limit', 0.0)) if pd.notna(row.get('high_limit', 0.0)) else 0.0
                    low_limit = float(row.get('low_limit', 0.0)) if pd.notna(row.get('low_limit', 0.0)) else 0.0
                    paused = bool(row.get('paused', False))
                else:
                    row = df.iloc[-1]
                    close_price = float(row['close']) if pd.notna(row['close']) else 0.0
                    if 'high_limit' in row:
                        high_limit = float(row.get('high_limit', 0.0)) if pd.notna(row.get('high_limit', 0.0)) else 0.0
                        low_limit = float(row.get('low_limit', 0.0)) if pd.notna(row.get('low_limit', 0.0)) else 0.0
                        paused = bool(row.get('paused', False))
                    else:
                        high_limit = close_price * 1.1
                        low_limit = close_price * 0.9
                        paused = False

                open_price = float(row['open']) if 'open' in row and pd.notna(row['open']) else None
                row_timestamp: Optional[datetime] = None
                if 'time' in row and pd.notna(row['time']):
                    try:
                        row_timestamp = pd.to_datetime(row['time'])
                    except Exception:
                        row_timestamp = None
                elif isinstance(df.index, pd.DatetimeIndex):
                    row_timestamp = df.index[-1].to_pydatetime()
                elif hasattr(df.index[-1], 'to_timestamp'):
                    row_timestamp = df.index[-1].to_timestamp()

                row_date = row_timestamp.date() if row_timestamp else None
                should_use_open = (
                    open_price is not None and use_open_price_window and current_date is not None and row_date == current_date
                )
                last_price = open_price if should_use_open else close_price

                data = SecurityUnitData(
                    security=security,
                    last_price=last_price,
                    high_limit=high_limit,
                    low_limit=low_limit,
                    paused=paused,
                )
            else:
                data = SecurityUnitData(security=security, last_price=0.0)
                log.debug(f"{security}无数据")

        except Exception as e:
            log.debug(f"获取{security}数据失败: {e}")
            data = SecurityUnitData(security=security, last_price=0.0)

        self._cache[cache_key] = data
        return data

    def __contains__(self, security: str) -> bool:
        try:
            data = self.__getitem__(security)
            return data.last_price > 0
        except Exception:
            return False

    def keys(self):
        return self._cache.keys()

    def items(self):
        return self._cache.items()

    def values(self):
        return self._cache.values()


class LiveCurrentData:
    """当前行情（实盘/tick 优先）。若 tick 不可用则回退到 BacktestCurrentData 逻辑。"""

    def __init__(self, context):
        self._context = context
        self._cache: Dict[Any, SecurityUnitData] = {}
        self._fallback = BacktestCurrentData(context)

    @staticmethod
    def _to_qmt_code(code: str) -> str:
        if code.endswith('.XSHE'):
            return code.replace('.XSHE', '.SZ')
        if code.endswith('.XSHG'):
            return code.replace('.XSHG', '.SH')
        return code

    def __getitem__(self, security: str) -> SecurityUnitData:
        current_dt = self._context.current_dt
        cache_key = (security, current_dt)
        if cache_key in self._cache:
            return self._cache[cache_key]

        requires_live = bool(getattr(_provider, 'requires_live_data', False))
        snap = None
        try:
            get_fn = getattr(_provider, 'get_live_current', None)
            if callable(get_fn):
                snap = get_fn(security)
        except Exception:
            if requires_live:
                raise

        if isinstance(snap, dict) and snap.get('last_price') is not None:
            data = SecurityUnitData(
                security=security,
                last_price=float(snap.get('last_price') or 0.0),
                high_limit=float(snap.get('high_limit') or 0.0),
                low_limit=float(snap.get('low_limit') or 0.0),
                paused=bool(snap.get('paused') or False),
            )
        else:
            if requires_live:
                provider_name = getattr(_provider, 'name', 'unknown')
                raise RuntimeError(
                    f"数据源 {provider_name} 未返回 {security} 的实时行情，请检查行情订阅/连接"
                )
            data = self._fallback[security]

        self._cache[cache_key] = data
        return data

    def __contains__(self, security: str) -> bool:
        try:
            data = self.__getitem__(security)
            return data.last_price > 0
        except Exception:
            return False

    def keys(self):
        return self._cache.keys()

    def items(self):
        return self._cache.items()

    def values(self):
        return self._cache.values()


def _get_setting(key: str, default: Any = False) -> Any:
    """统一获取设置项"""
    return get_settings().options.get(key, default)


def _coerce_date(value: Any) -> Optional[Date]:
    if value in (None, "", "NaT"):
        return None
    if isinstance(value, Date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        parsed = pd.to_datetime(value)
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return parsed.date()


def _normalize_security_info(security: str, raw_info: Any) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    if isinstance(raw_info, dict):
        normalized.update({k: v for k, v in raw_info.items() if v is not None})
    else:
        for attr in (
            'type',
            'subtype',
            'display_name',
            'name',
            'start_date',
            'end_date',
            'parent',
        ):
            if hasattr(raw_info, attr):
                value = getattr(raw_info, attr)
                if value is not None:
                    normalized[attr] = value

    normalized.setdefault('code', security)

    for field in ('start_date', 'end_date'):
        normalized[field] = _coerce_date(normalized.get(field))

    if normalized.get('end_date') is None:
        normalized['end_date'] = Date(2200, 1, 1)

    return normalized


def get_security_info(security: str) -> SecurityInfo:
    """
    获取标的的基础信息（如类型、子类型），结果会缓存。
    若当前数据源不支持，返回空对象。
    """
    if not security:
        return SecurityInfo('', {})

    cached = _security_info_cache.get(security)
    if cached is not None:
        return cached

    _ensure_auth()

    info_fn = getattr(_provider, 'get_security_info', None)
    if not callable(info_fn):
        empty = SecurityInfo(security, {})
        _security_info_cache[security] = empty
        return empty

    try:
        raw_info = info_fn(security)
    except Exception as exc:
        log.debug(f"获取{security}基本信息失败: {exc}")
        raw_info = {}

    normalized = _normalize_security_info(security, raw_info)
    # 应用配置覆盖（分类/tplus/slippage等）
    normalized = _merge_overrides(security, normalized)
    info_obj = SecurityInfo(security, normalized)
    _security_info_cache[security] = info_obj
    return info_obj


def _coerce_price_result_to_dataframe(result: Any) -> pd.DataFrame:
    """将 provider.get_price 的返回结果统一为 DataFrame，容忍 Panel/长表/宽表/字典等形式。
    不负责最终的字段 MultiIndex 兼容变换，仅做基础整形。
    """
    # Panel-like
    if hasattr(result, 'to_frame'):
        try:
            return result.to_frame()
        except Exception as e:
            log.debug(f"to_frame 失败: {e}")
            try:
                return pd.DataFrame(result)
            except Exception:
                return pd.DataFrame()
    # DataFrame
    if isinstance(result, pd.DataFrame):
        # 长表 (time, code, value...)
        if 'time' in result.columns and 'code' in result.columns:
            try:
                value_columns = [c for c in result.columns if c not in ['time', 'code']]
                if len(value_columns) == 1:
                    pivot_df = result.pivot(index='time', columns='code', values=value_columns[0])
                    pivot_df.columns = pd.MultiIndex.from_product(
                        [value_columns, pivot_df.columns.tolist()], names=['field', 'code']
                    )
                    if not isinstance(pivot_df.index, pd.DatetimeIndex):
                        pivot_df.index = pd.to_datetime(pivot_df.index)
                    return pivot_df
                # 多字段长表
                parts = [result.pivot(index='time', columns='code', values=col) for col in value_columns]
                df_wide = pd.concat(parts, axis=1, keys=value_columns)
                df_wide.columns.names = ['field', 'code']
                if not isinstance(df_wide.index, pd.DatetimeIndex):
                    df_wide.index = pd.to_datetime(df_wide.index)
                return df_wide
            except Exception as e:
                log.debug(f"长表转换失败: {e}")
                return result
        # 宽表直接返回
        return result
    # 字典或其他
    try:
        df = pd.DataFrame(result)
        if 'time' in df.columns:
            try:
                df['time'] = pd.to_datetime(df['time'])
                df.set_index('time', inplace=True)
            except Exception:
                pass
        return df
    except Exception as e:
        log.debug(f"无法将结果转换为 DataFrame: {e}")
        return pd.DataFrame()


def get_price(
    security: Union[str, List[str]],
    start_date: Optional[Union[str, datetime]] = None,
    end_date: Optional[Union[str, datetime]] = None,
    frequency: str = 'daily',
    fields: Optional[List[str]] = None,
    skip_paused: bool = False,
    fq: str = 'pre',
    count: Optional[int] = None,
    panel: bool = True,
    fill_paused: bool = True
) -> pd.DataFrame:
    """
    获取历史数据（避免未来函数，支持真实价格）
    
    Args:
        security: 标的代码或代码列表
        start_date: 开始日期
        end_date: 结束日期（会自动限制在当前回测时间之前）
        frequency: 频率 ('daily', '1d', 'minute', '1m')
        fields: 字段列表 ['open', 'close', 'high', 'low', 'volume', 'money']
        skip_paused: 是否跳过停牌
        fq: 复权方式 ('pre'-前复权, 'post'-后复权, None-不复权)
        count: 获取数量
        panel: 是否返回panel格式
        fill_paused: 是否填充停牌数据
        
    Returns:
        DataFrame
        
    Raises:
        FutureDataError: 当 avoid_future_data=True 时访问未来数据
    """
    # 确保数据提供者已认证
    _ensure_auth()
    
    if not _current_context:
        # 没有回测上下文，直接调用原始API
        return _provider.get_price(
            security=security,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            fields=fields,
            skip_paused=skip_paused,
            fq=fq,
            count=count,
            panel=panel,
            fill_paused=fill_paused
        )
    
    avoid_future = _get_setting('avoid_future_data')
    use_real_price = _get_setting('use_real_price')
    
    # 处理 end_date
    if end_date is None:
        end_date = datetime(2030, 12, 31)  # 默认一个很远的未来日期
    elif isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
    elif isinstance(end_date, Date) and not isinstance(end_date, datetime):
        end_date = datetime.combine(end_date, Time(15, 0))
    
    current_dt = _current_context.current_dt
    
    # 标准化频率
    frequency_map = {'daily': '1d', 'minute': '1m'}
    freq = frequency_map.get(frequency, frequency)
    
    # 避免未来数据检查
    if avoid_future:
        if fields is None:
            fields = ['open', 'close', 'high', 'low', 'volume', 'money']
        
        # 检查 end_date 是否超过当前时间
        if 'm' in freq:
            # 分钟数据
            if end_date > current_dt:
                raise FutureDataError(
                    f"avoid_future_data=True时，get_price的end_date({end_date})"
                    f"不能大于当前时间({current_dt})"
                )
        elif 'd' in freq:
            # 日数据
            if end_date.date() > current_dt.date():
                raise FutureDataError(
                    f"avoid_future_data=True时，get_price的end_date({end_date.date()})"
                    f"不能大于当前日期({current_dt.date()})"
                )
            elif end_date.date() == current_dt.date():
                # 同一天，需要检查时间和字段
                _check_intraday_future_data(current_dt, fields, end_date)
    
    # 限制 end_date 不超过当前时间
    end_date = min(end_date, current_dt)
    
    # 真实价格模式：使用当前回测时间作为复权参考日期
    if use_real_price and fq == 'pre':
        # 使用当前回测日期作为复权参考日期，以获得当时的真实价格
        
        # 确保 pre_factor_ref_date 是 datetime.date 类型
        if isinstance(current_dt, Date) and not isinstance(current_dt, datetime):
            pre_factor_ref_date = current_dt
        elif isinstance(current_dt, datetime):
            pre_factor_ref_date = current_dt.date()
        else:
            try:
                pre_factor_ref_date = pd.to_datetime(current_dt).date()
            except:
                log.warning(f"无法转换 current_dt 为 date 类型: {current_dt}, 使用今天日期")
                pre_factor_ref_date = Date.today()
        
        try:
            # 真实价格模式优先使用提供者内部引擎支持
            #log.debug(f"调用 provider.get_price(prefer_engine=True): security={security}, fields={fields}")
            result = _provider.get_price(
                security=security,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
                fields=fields,
                skip_paused=skip_paused,
                fq=fq,
                count=count,
                panel=panel,
                fill_paused=fill_paused,
                prefer_engine=True,
                pre_factor_ref_date=pre_factor_ref_date
            )
            #log.debug(f"provider.get_price 返回: {type(result)}, {result.shape if hasattr(result, 'shape') else 'No shape'}")
            return result
            # df = _coerce_price_result_to_dataframe(result) # (tyb)TODO 这里会更改返回的数据类型，和平台上的对不齐，需要回退
            # return _make_compatible_dataframe(df, fields)
            
        except Exception as e:
            log.warning(f"真实价格模式调用失败: {e}，回退到标准复权")
    
    # 标准模式或真实价格模式失败时的回退策略
    try:
        df = _provider.get_price(
            security=security,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            fields=fields,
            skip_paused=skip_paused,
            fq=fq,
            count=count,
            panel=panel,
            fill_paused=fill_paused
        )
        return df

        # 兼容性处理：让多证券情况下也能通过 df['close'] 访问
        # return _make_compatible_dataframe(df, fields)
        
    except Exception as e:
        log.error(f"获取价格数据失败: {e}")
        return pd.DataFrame()

def get_concept_stocks(
    concept_code: list = [],
    date: Optional[Union[str, datetime]] = None
) -> list:
    # 确保数据提供者已认证
    _ensure_auth()

    return _provider.get_concept_stocks(
            concept_code=concept_code,
            date=date,
    )
    
    # (tyb)TODO 可能需要考虑一些兼容性的问题，等碰到了再解决


def get_factor_values(
    securities: Union[str, List[str]],
    factors: Union[str, List[str]],
    start_date: Optional[Union[str, datetime]] = None,
    end_date: Optional[Union[str, datetime]] = None,
    count:Optional[int] = None
) -> Dict[str, pd.DataFrame]:
    """
    获取因子数据（避免未来函数）
    
    Args:
        securities:股票池，单只股票（字符串）或一个股票列表
        factors: 因子名称，单个因子（字符串）或一个因子列表
        start_date:开始日期，字符串或 datetime 对象，与 coun t参数二选一
        end_date: 结束日期， 字符串或 datetime 对象，可以与 start_date 或 count 配合使用
        count: 截止 end_date 之前交易日的数量（含 end_date 当日），与 start_date 参数二选一
        
    Returns:
        一个 dict： key 是因子名称， value 是 pandas.dataframe。
        dataframe 的 index 是日期， column 是股票代码， value 是因子值
    """

    # 确保数据提供者已认证
    _ensure_auth()

    return _provider.get_factor_values(
        securities=securities,
        factors=factors,
        start_date=start_date,
        end_date=end_date,
        count=count
    )        


def _make_compatible_dataframe(df: pd.DataFrame, fields: Optional[List[str]]) -> pd.DataFrame:
    """
    让 DataFrame 兼容策略代码的访问方式
    
    目标：让多证券情况下也能通过 df['close'] 访问到数据
    
    Args:
        df: 原始 DataFrame
        fields: 请求的字段列表
        
    Returns:
        兼容的 DataFrame（列为 MultiIndex(field, code)），保证 df['close'] 返回二维矩阵
    """
    if df.empty:
        return df

    known_fields = {
        'open', 'close', 'high', 'low', 'volume', 'money',
        'avg', 'price', 'high_limit', 'low_limit', 'paused'
    }

    # 情况1：索引是 (code, time) 的 MultiIndex（panel=False 常见返回）
    if isinstance(df.index, pd.MultiIndex) and set(df.index.names or []) >= {'code', 'time'}:
        try:
            df_reset = df.reset_index()
            # 确定值字段列表
            if fields and len(fields) > 0:
                value_columns = [col for col in fields if col in df_reset.columns]
            else:
                value_columns = [c for c in df_reset.columns if c not in ['code', 'time']]
            if not value_columns:
                return df

            wide_list = []
            for col in value_columns:
                wide_col = df_reset.pivot(index='time', columns='code', values=col)
                wide_list.append(wide_col)

            if len(wide_list) == 1:
                wide = wide_list[0]
                wide.columns = pd.MultiIndex.from_product(
                    [value_columns, wide.columns.tolist()], names=['field', 'code']
                )
            else:
                wide = pd.concat(wide_list, axis=1, keys=value_columns)
                wide.columns.names = ['field', 'code']

            if not isinstance(wide.index, pd.DatetimeIndex):
                wide.index = pd.to_datetime(wide.index)
            return wide
        except Exception:
            return df

    # 情况2：列是 MultiIndex（但层级顺序可能是 (code, field)）
    if isinstance(df.columns, pd.MultiIndex):
        try:
            level0 = set(map(str, df.columns.get_level_values(0)))
            level1 = set(map(str, df.columns.get_level_values(1)))
            # 期望：field 在外层。如果 field 出现在第2层而非第1层，则交换层级。
            if (level1 & known_fields) and not (level0 & known_fields):
                df = df.swaplevel(0, 1, axis=1)
            df.columns.names = ['field', 'code']
            return df
        except Exception:
            return df

    # 情况3：普通列，但为多证券+单字段（列是证券代码集合）
    if fields and len(fields) == 1 and df.shape[1] > 1:
        try:
            df.columns = pd.MultiIndex.from_product(
                [fields, list(map(str, df.columns))], names=['field', 'code']
            )
            return df
        except Exception:
            return df

    # 其他情况（单证券或已是期望结构）直接返回
    return df


def _check_intraday_future_data(current_dt: datetime, fields: List[str], end_date: datetime):
    """
    检查盘中是否访问未来数据
    
    Args:
        current_dt: 当前回测时间
        fields: 请求的字段
        end_date: 结束日期
        
    Raises:
        FutureDataError: 如果访问了未来数据
    """
    # 交易时间段
    market_open = Time(9, 30)
    market_close = Time(15, 0)
    
    current_time = current_dt.time()
    
    # 盘前（开盘前）
    if current_time < market_open:
        future_fields = set(fields) & {
            'open', 'close', 'high', 'low', 'volume', 'money', 'avg', 'price'
        }
        if future_fields:
            raise FutureDataError(
                f"avoid_future_data=True时，当天开盘前不能获取当日的{future_fields}字段数据，"
                f"current_dt={current_dt}, end_date={end_date}"
            )
    
    # 盘中（开盘后、收盘前）
    elif market_open <= current_time < market_close:
        future_fields = set(fields) & {
            'close', 'high', 'low', 'volume', 'money', 'avg', 'price'
        }
        if future_fields:
            raise FutureDataError(
                f"avoid_future_data=True时，盘中不能取当日的{future_fields}字段数据，"
                f"current_dt={current_dt}, end_date={end_date}"
            )
    
    # 盘后（收盘后）- 可以获取所有数据
    else:
        pass


def attribute_history(
    security: str,
    count: int,
    unit: str = '1d',
    fields: Optional[List[str]] = None,
    skip_paused: bool = False,
    df: bool = True,
    fq: str = 'pre'
) -> Union[pd.DataFrame, Dict]:
    """
    获取单个标的历史数据（避免未来函数，支持真实价格）。

    Args:
        security: 标的代码
        count: 获取数量
        unit: 时间单位 ('1d', '1m')
        fields: 字段列表
        skip_paused: 是否跳过停牌
        df: 是否返回DataFrame
        fq: 复权方式

    Returns:
        DataFrame或Dict
    """
    if not _current_context:
        end_date = datetime.now()
    else:
        end_date = _current_context.current_dt
        if 'm' in unit:
            end_date = end_date + timedelta(minutes=1)
        elif 'd' in unit:
            end_date = end_date - timedelta(days=1)

    frequency = 'daily' if 'd' in unit else 'minute'

    try:
        return get_price(
            security=security,
            end_date=end_date,
            frequency=frequency,
            fields=fields,
            skip_paused=skip_paused,
            fq=fq,
            count=count
        )
    except Exception as e:
        log.error(f"获取历史数据失败: {e}")
        return pd.DataFrame() if df else {}


def _should_use_live_current() -> bool:
    """判断是否使用 LiveCurrentData：只要是实盘则启用。"""
    try:
        from ..core.globals import g  # type: ignore
        return bool(getattr(g, 'live_trade', False))
    except Exception:
        return False


def get_current_data() -> Any:
    """
    获取当前行情数据容器（避免未来函数）
    
    返回一个CurrentData对象，支持延迟加载。
    当访问 current_data[security] 时才真正获取该标的的数据。
    
    Returns:
        CurrentData对象，支持字典式访问
        
    Examples:
        >>> current_data = get_current_data()
        >>> price = current_data['000001.XSHE'].last_price
        >>> if '000001.XSHE' in current_data:
        >>>     print(current_data['000001.XSHE'].paused)
    """
    if not _current_context:
        # 返回一个空的CurrentData对象
        class EmptyCurrentData:
            def __getitem__(self, key):
                return SecurityUnitData(security=key, last_price=0.0)
            def __contains__(self, key):
                return False
            def keys(self):
                return []
        return EmptyCurrentData()
    
    return LiveCurrentData(_current_context) if _should_use_live_current() else BacktestCurrentData(_current_context)


def get_trade_days(
    start_date: Optional[Union[str, datetime]] = None,
    end_date: Optional[Union[str, datetime]] = None,
    count: Optional[int] = None
) -> List[datetime]:
    """
    获取交易日列表（避免未来函数）
    
    Args:
        start_date: 开始日期
        end_date: 结束日期（会自动限制在当前回测时间之前）
        count: 获取数量
        
    Returns:
        交易日列表
    """
    # 确保不会获取未来数据
    if _current_context:
        max_date = _current_context.current_dt
        if end_date:
            if isinstance(end_date, str):
                end_date = pd.to_datetime(end_date)
            end_date = min(end_date, max_date)
        else:
            end_date = max_date
    
    try:
        trade_days = _provider.get_trade_days(
            start_date=start_date,
            end_date=end_date,
            count=count
        )
        return [pd.to_datetime(d) for d in trade_days]
    except Exception as e:
        log.error(f"获取交易日失败: {e}")
        return []


def get_all_securities(
    types: Union[str, List[str]] = 'stock',
    date: Optional[Union[str, datetime]] = None
) -> pd.DataFrame:
    """
    获取所有标的信息
    
    Args:
        types: 标的类型 ('stock', 'fund', 'index', 'futures', 'etf', 'lof', 'fja', 'fjb')
        date: 日期（会自动限制在当前回测时间之前）
        
    Returns:
        DataFrame
    """
    if _current_context and date is None:
        date = _current_context.current_dt
    
    try:
        return _provider.get_all_securities(types=types, date=date)
    except Exception as e:
        log.error(f"获取标的信息失败: {e}")
        return pd.DataFrame()


def get_index_stocks(
    index_symbol: str,
    date: Optional[Union[str, datetime]] = None
) -> List[str]:
    """
    获取指数成分股
    
    Args:
        index_symbol: 指数代码
        date: 日期（会自动限制在当前回测时间之前）
        
    Returns:
        成分股代码列表
    """
    if _current_context and date is None:
        date = _current_context.current_dt
    
    try:
        return _provider.get_index_stocks(index_symbol, date=date)
    except Exception as e:
        log.error(f"获取指数成分股失败: {e}")
        return []


def get_tick_decimals(security: str) -> int:
    """获取标的的价格精度（小数位数）。
    
    根据 security_overrides.json 的配置确定：
    - stock: 2位小数（0.01步长）
    - fund/money_market_fund: 3位小数（0.001步长）
    
    Args:
        security: 证券代码，如 '512100.XSHG'
    
    Returns:
        int: 价格精度，2 或 3
    """
    info = get_security_info(security)
    category = str(info.get('category') or '').lower()
    if category in ('fund', 'money_market_fund'):
        return 3
    return 2


__all__ = [
    'get_price', 'attribute_history', 'get_current_data',
    'get_trade_days', 'get_all_securities', 'get_index_stocks',
    'set_current_context', 'set_data_provider', 'get_data_provider',
    'set_security_overrides', 'reset_security_overrides',
    'get_tick_decimals',
]


# =========================
# 分红/拆分数据获取（统一结构）
# =========================
def _to_date(d: Optional[Union[str, datetime, Date]]) -> Optional[Date]:
    if d is None:
        return None
    if isinstance(d, Date) and not isinstance(d, datetime):
        return d
    try:
        return pd.to_datetime(d).date()
    except Exception:
        return None


def _infer_security_type(security: str, ref_date: Optional[Date]) -> str:
    """
    通过 get_all_securities 推断标的类型。
    返回值之一：'stock', 'etf', 'lof', 'fund', 'fja', 'fjb'
    未识别则默认 'stock'。
    """
    try:
        check_date = ref_date or (_current_context.current_dt.date() if _current_context else None)
        for t in ['stock', 'etf', 'lof', 'fund', 'fja', 'fjb']:
            df = _provider.get_all_securities(types=t, date=check_date)
            if not df.empty and security in df.index:
                return t
    except Exception:
        pass
    return 'stock'


def _parse_dividend_note(note: str) -> Dict[str, Any]:
    """
    解析股票分红文字说明，提取每10股的送股、转增、派现（税前）数值。
    返回字典：{'per_base': 10, 'stock_paid': float, 'into_shares': float, 'bonus_pre_tax': float}
    若出现“每股”则按每股计，per_base=1。
    该解析为启发式，尽量覆盖常见格式：
    - “每10股派X元(含税)” / “10派X元” / “派X元(每10股)”
    - “每10股送X股” / “10送X股”
    - “每10股转增X股” / “10转X股” / “转增X股(每10股)”
    """
    result = {'per_base': 10, 'stock_paid': 0.0, 'into_shares': 0.0, 'bonus_pre_tax': 0.0}
    if not note:
        return result
    s = note.replace('（', '(').replace('）', ')').replace('，', ',').replace('。', '.')
    s = re.sub(r'\s+', '', s)

    # 基数：每股 或 每10股
    if '每股' in s:
        result['per_base'] = 1
    elif '每10股' in s or re.search(r'(^|[^\d])10(送|转|派)', s):
        result['per_base'] = 10

    # 派现（税前）
    m = re.search(r'(每10股|10派|派)(?P<val>\d+(?:\.\d+)?)(元|现金)?', s)
    if not m and '每股' in s:
        m = re.search(r'每股派(?P<val>\d+(?:\.\d+)?)(元|现金)?', s)
        if m:
            # 每股派 -> 每10股派
            result['bonus_pre_tax'] = float(m.group('val')) * 10
    elif m:
        result['bonus_pre_tax'] = float(m.group('val'))

    # 送股
    m = re.search(r'(每10股|10送|送)(?P<val>\d+(?:\.\d+)?)(股)?', s)
    if not m and '每股' in s:
        m = re.search(r'每股送(?P<val>\d+(?:\.\d+)?)(股)?', s)
        if m:
            result['stock_paid'] = float(m.group('val')) * 10
    elif m:
        result['stock_paid'] = float(m.group('val'))

    # 转增
    m = re.search(r'(每10股|10转|转增|转)(?P<val>\d+(?:\.\d+)?)(股)?', s)
    if not m and '每股' in s:
        m = re.search(r'每股转增(?P<val>\d+(?:\.\d+)?)(股)?', s)
        if m:
            result['into_shares'] = float(m.group('val')) * 10
    elif m:
        result['into_shares'] = float(m.group('val'))

    return result


def get_split_dividend(
    security: str,
    start_date: Optional[Union[str, datetime, Date]] = None,
    end_date: Optional[Union[str, datetime, Date]] = None
) -> List[Dict[str, Any]]:
    """
    获取指定标的在区间内的分红/拆分事件（统一结构）。

    返回的每个事件为字典：
    - 'security': 代码
    - 'date': 日期（ex_date 或 day）
    - 'security_type': 'stock'/'etf'/'lof'/'fund'/'fja'/'fjb'
    - 'scale_factor': float，拆分/送转后持仓系数；无则为1.0
    - 'bonus_pre_tax': float，每10股（或每份）派现金额（税前）
    - 'per_base': int，基数（股票通常为10，货币基金为1）
    """
    # 统一日期
    sd = _to_date(start_date)
    ed = _to_date(end_date)
    if _current_context:
        cd = _current_context.current_dt.date()
        if ed is None or ed > cd:
            ed = cd
        if sd is None:
            sd = ed
    elif sd is None or ed is None:
        raise UserError('get_split_dividend 需要提供开始/结束日期，或在回测上下文中调用')

    # 直接通过当前数据提供者获取标准化事件
    try:
        return _provider.get_split_dividend(security, start_date=sd, end_date=ed)
    except Exception as e:
        log.debug(f"获取分红数据失败[{security}]: {e}")
        return []
