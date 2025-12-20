import os
import pickle
import datetime
import threading
from typing import Optional, List, Dict, Any, Union, Tuple
from abc import ABC, abstractmethod
import pandas as pd

import jqdatasdk as jq
from jqdatasdk import finance, query

from .base import DataProvider

# ========================
# 配置 & 工具
# ========================
CACHE_DIR = "cache/jqdata_v3"
CACHE_STATS = {
    'hits': 0,
    'misses': 0,
    'total_requests': 0,
    'lock': threading.Lock()
}

def normalize_frequency(freq: str) -> str:
    """标准化频率字符串"""
    freq = freq.lower().strip()
    mapping = {
        'd': 'daily',
        '1d': 'daily',
        'day': 'daily',
        'w': 'weekly',
        '1w': 'weekly',
        'week': 'weekly',
        'm': 'monthly',
        '1m': '1m',  # 注意：1m 是分钟，不是月！
        'month': 'monthly',
    }
    # 分钟线保留原样（如 '5m'）
    if freq.endswith('m') and freq[:-1].isdigit():
        return freq
    return mapping.get(freq, freq)

def is_minute_frequency(freq: str) -> bool:
    return freq.endswith('m') and freq[:-1].isdigit()

def align_range_by_frequency(
    start: datetime.date,
    end: datetime.date,
    freq: str
) -> Tuple[datetime.date, datetime.date]:
    """根据频率扩展缓存范围"""
    if is_minute_frequency(freq):
        # 分钟线：按自然月缓存（避免跨月文件太大）
        start = datetime.date(start.year, start.month, 1)
        if end.month == 12:
            end = datetime.date(end.year, 12, 31)
        else:
            next_month = datetime.date(end.year, end.month + 1, 1)
            end = next_month - datetime.timedelta(days=1)
    else:
        # 日线/周线/月线：按年缓存
        start = datetime.date(start.year, 1, 1)
        end = datetime.date(end.year, 12, 31)
    return start, end

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def to_date_str(d: Union[str, datetime.date, datetime.datetime]) -> str:
    if isinstance(d, str):
        return d
    return d.strftime('%Y-%m-%d')

def parse_date(d: Union[str, datetime.date, datetime.datetime]) -> datetime.date:
    if isinstance(d, datetime.datetime):
        return d.date()
    if isinstance(d, datetime.date):
        return d
    return datetime.datetime.strptime(d, '%Y-%m-%d').date()

def align_to_quarter_range(start: datetime.date, end: datetime.date) -> Tuple[datetime.date, datetime.date]:
    """扩展为整季度范围，平衡缓存大小与复用率"""
    def quarter_start(d):
        q = (d.month - 1) // 3
        return datetime.date(d.year, q * 3 + 1, 1)
    def quarter_end(d):
        q = (d.month - 1) // 3
        month = (q + 1) * 3
        year = d.year
        if month > 12:
            month = 12
        # 最后一天
        if month in (1, 3, 5, 7, 8, 10, 12):
            day = 31
        elif month in (4, 6, 9, 11):
            day = 30
        else:  # Feb
            day = 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28
        return datetime.date(year, month, day)

    return quarter_start(start), quarter_end(end)


def record_cache_hit():
    with CACHE_STATS['lock']:
        CACHE_STATS['total_requests'] += 1
        CACHE_STATS['hits'] += 1

def record_cache_miss():
    with CACHE_STATS['lock']:
        CACHE_STATS['total_requests'] += 1
        CACHE_STATS['misses'] += 1

def get_cache_stats():
    with CACHE_STATS['lock']:
        total = CACHE_STATS['total_requests']
        hit_rate = CACHE_STATS['hits'] / total if total > 0 else 0
        return {
            'total_requests': total,
            'hits': CACHE_STATS['hits'],
            'misses': CACHE_STATS['misses'],
            'hit_rate': f"{hit_rate:.2%}"
        }

# ========================
# 缓存读写（带简单锁）
# ========================
def save_pickle_safe(obj, path, max_retries=3):
    for i in range(max_retries):
        try:
            with open(path, 'wb') as f:
                pickle.dump({'data': obj, 'timestamp': datetime.datetime.now()}, f)
            break
        except Exception as e:
            if i == max_retries - 1:
                raise e

def load_pickle_safe(path, max_age_days=30):
    if not os.path.exists(path):
        return None
    try:
        with open(path, 'rb') as f:
            cache = pickle.load(f)
        if (datetime.datetime.now() - cache['timestamp']).days > max_age_days:
            os.remove(path)
            return None
        return cache['data']
    except Exception:
        return None

# ========================
# 主类
# ========================
class JQDataCacheProvider(DataProvider):
    name: str = "jqdatacache"
    _DEFAULT_PRICE_FIELDS: List[str] = ['open', 'close', 'high', 'low', 'volume', 'money']
    _PRICE_SCALE_FIELDS: set = {
        'open', 'close', 'high', 'low', 'avg', 'price', 'high_limit', 'low_limit', 'pre_close'
    }

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        super().__init__()
        self.config = config or {}
        ensure_dir(os.path.join(CACHE_DIR, "price"))
        ensure_dir(os.path.join(CACHE_DIR, "trade_days"))
        ensure_dir(os.path.join(CACHE_DIR, "all_securities"))
        ensure_dir(os.path.join(CACHE_DIR, "index_stocks"))
        ensure_dir(os.path.join(CACHE_DIR, "concept_stocks"))
        ensure_dir(os.path.join(CACHE_DIR, "split_dividend"))

    @staticmethod
    def _to_date(d: Optional[Union[str, datetime.datetime, datetime.date]]) -> Optional[datetime.datetime]:
        if d is None:
            return None
        if isinstance(d, datetime.date) and not isinstance(d, datetime.datetime):
            return d
        try:
            return pd.to_datetime(d).date()
        except Exception:
            return None

    def _infer_security_type(self, security: str, ref_date: Optional[datetime.date]) -> str:
        try:
            for t in ['stock', 'etf', 'lof', 'fund', 'fja', 'fjb']:
                df = self.get_all_securities(types=t, date=ref_date)
                if not df.empty and security in df.index:
                    return t
        except Exception:
            pass
        return 'stock'

    @staticmethod
    def _sanitize_env_value(value: str) -> str:
        return value.split('#', 1)[0].strip()

    @staticmethod
    def _parse_host(host: Optional[str], env_value: str) -> str:
        if host:
            return host
        return JQDataCacheProvider._sanitize_env_value(env_value)

    @staticmethod
    def _parse_port(port: Optional[int], env_value: str) -> Optional[int]:
        if port is not None:
            return port
        cleaned = JQDataCacheProvider._sanitize_env_value(env_value)
        if not cleaned:
            return None
        try:
            return int(cleaned)
        except ValueError:
            logger.warning("Invalid JQDATA_PORT value '%s'; ignoring custom port.", env_value)
            return None

    def auth(self, user: Optional[str] = None, pwd: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None) -> None:
        cfg_server = self.config.get('server') or os.getenv('JQDATA_SERVER', '')
        jq_server = self._parse_host(host, cfg_server)
        cfg_port = self.config.get('port')
        jq_port_env = str(cfg_port) if cfg_port is not None else os.getenv('JQDATA_PORT', '')
        jq_port = self._parse_port(port, jq_port_env)
        jq_user = (
            user
            or self.config.get('username')
            or os.getenv('JQDATA_USERNAME')
            or os.getenv('JQDATA_USER', '')
        )
        jq_pwd = (
            pwd
            or self.config.get('password')
            or os.getenv('JQDATA_PASSWORD')
            or os.getenv('JQDATA_PWD', '')
        )
        # 允许空host/port走默认
        if jq_user:
            try:
                if jq_server and jq_port:
                    jq.auth(jq_user, jq_pwd, host=jq_server, port=jq_port)
                else:
                    jq.auth(jq_user, jq_pwd)
            except Exception as e:
                # 忽略认证失败，由调用方处理
                logger.error("Failed to authenticate with JQData: %s", e)
                raise e

    # =================================================================
    # get_price with smart caching and auto-fill
    # =================================================================
    def get_price(
        self,
        security: Union[str, List[str]],
        start_date: Optional[Union[str, datetime.datetime]] = None,
        end_date: Optional[Union[str, datetime.datetime]] = None,
        frequency: str = 'daily',
        fields: Optional[List[str]] = None,
        skip_paused: bool = False,
        fq: str = 'pre',
        count: Optional[int] = None,
        panel: bool = True,
        fill_paused: bool = True,
        pre_factor_ref_date: Optional[Union[str, datetime.datetime]] = None,
        prefer_engine: bool = False,
    ) -> pd.DataFrame:
        fields_with_code  = fields + ["code"] if fields and "code" not in fields else fields
        fields_with_code += ["factor"]

        # 标准化频率
        freq = normalize_frequency(frequency)
        is_min = is_minute_frequency(freq)

        securities = [security] if isinstance(security, str) else security

        # 确定时间范围（注意：分钟线需 datetime，日线可用 date）
        if end_date is None:
            end_date = datetime.datetime.now() if is_min else datetime.date.today()
        if isinstance(end_date, datetime.date) and not isinstance(end_date, datetime.datetime):
            if is_min:
                end_date = datetime.datetime.combine(end_date, datetime.time(15, 0))  # A股收盘
            else:
                end_date = end_date

        if start_date is None:
            if count is not None:
                if is_min:
                    # 粗略估计：1天 ≈ 240 分钟（A股）
                    days = max(1, count // 240 + 2)
                    start_date = end_date - datetime.timedelta(days=days)
                else:
                    start_date = end_date - datetime.timedelta(days=count * 2)
            else:
                start_date = datetime.datetime(2015, 1, 1) if is_min else datetime.date(2005, 1, 1)
        else:
            if isinstance(start_date, str):
                start_date = datetime.datetime.fromisoformat(start_date) if is_min else parse_date(start_date)
            elif isinstance(start_date, datetime.date) and not isinstance(start_date, datetime.datetime):
                start_date = datetime.datetime.combine(start_date, datetime.time(9, 30)) if is_min else start_date

        # 对齐到最近的交易日
        trade_days = self.get_trade_days()
        trade_days = sorted([d.date() if isinstance(d, datetime.datetime) else d for d in trade_days])
        def get_nearest_trade_day(target, direction='forward'):
            # direction: 'forward' 表示向后找 >= target，'backward' 表示向前找 <= target
            for d in (trade_days if direction == 'forward' else reversed(trade_days)):
                if (direction == 'forward' and d >= target) or (direction == 'backward' and d <= target):
                    return d
            return trade_days[0] if direction == 'forward' else trade_days[-1]

        req_start = get_nearest_trade_day(start_date.date(), direction='forward')
        req_end = get_nearest_trade_day(end_date.date(), direction='backward')

        req_start = datetime.datetime.combine(req_start, datetime.time(9, 30))
        req_end = datetime.datetime.combine(req_end, datetime.time(15, 0))

        all_dfs = []
        for sec in securities:
            # 缓存路径：按频率分目录
            freq_dir = os.path.join(CACHE_DIR, "price", freq)
            ensure_dir(freq_dir)

            if is_min:
                # 分钟线：按年月分文件（如 000001_2025_06.pkl）
                year_month = req_end.strftime("%Y_%m") if isinstance(req_end, datetime.datetime) else req_end.strftime("%Y_%m")
                cache_filename = f"{sec.replace('.', '_')}_{year_month}.pkl"
            else:
                # 日线：按股票+频率+fq
                cache_filename = f"{sec.replace('.', '_')}_{freq}_{fq}.pkl"

            cache_path = os.path.join(freq_dir, cache_filename)

            cached_df = load_pickle_safe(cache_path)
            need_fetch_ranges = []
            df_final = None

            if cached_df is not None:
                if not isinstance(cached_df['time'], pd.DatetimeIndex):
                    cached_df['time'] = pd.to_datetime(cached_df['time'])
                cached_df = cached_df.sort_values(by='time')

                cache_min = datetime.datetime.combine(cached_df['time'].min().date(), datetime.time(9, 30))
                cache_max = datetime.datetime.combine(cached_df['time'].max().date(), datetime.time(15, 0))

                # 获取交易日列表
                trade_days_set = set(trade_days)
                def get_prev_trade_day(dt):
                    d = dt.date() if isinstance(dt, datetime.datetime) else dt
                    prev_days = [day for day in trade_days if day < d]
                    if prev_days:
                        return datetime.datetime.combine(prev_days[-1], datetime.time(9, 30))
                    return None
                def get_next_trade_day(dt):
                    d = dt.date() if isinstance(dt, datetime.datetime) else dt
                    next_days = [day for day in trade_days if day > d]
                    if next_days:
                        return datetime.datetime.combine(next_days[0], datetime.time(9, 30))
                    return None

                # 判断是否覆盖
                if cache_min <= req_start and cache_max >= req_end:
                    if is_min:
                        df_final = cached_df[(cached_df['time'] >= req_start) & (cached_df['time'] <= req_end)]
                    else:
                        df_final = cached_df[(cached_df['time'].dt.date >= req_start.date()) & (cached_df['time'].dt.date <= req_end.date())]
                    record_cache_hit()
                else:
                    record_cache_miss()
                    if req_start < cache_min:
                        prev_trade_dt = get_prev_trade_day(cache_min)
                        if prev_trade_dt is not None:
                            need_fetch_ranges.append((req_start, prev_trade_dt))
                        else:
                            need_fetch_ranges.append((req_start, cache_min))
                    if req_end > cache_max:
                        next_trade_dt = get_next_trade_day(cache_max)
                        if next_trade_dt is not None:
                            need_fetch_ranges.append((next_trade_dt, req_end))
                        else:
                            need_fetch_ranges.append((cache_max, req_end))
                    df_final = cached_df.copy()
            else:
                record_cache_miss()
                need_fetch_ranges = [(req_start, req_end)]
                df_final = None

            # 拉取缺失范围
            for (s, e) in need_fetch_ranges:
                if s > e:
                    continue
                try:
                    print(1)
                    new_df = self._fetch_price_from_remote(
                        security=[sec],
                        start_date=s,
                        end_date=e,
                        frequency=freq,
                        fields=['open','close','low','high','volume','money','factor','high_limit','low_limit','avg','pre_close','paused'],
                        fq=fq
                    )
                    if df_final is None:
                        df_final = new_df
                    else:
                        df_final = pd.concat([df_final, new_df]).sort_values(by='time').drop_duplicates()
                except Exception as ex:
                    raise RuntimeError(f"Fetch failed for {sec} ({s} to {e}): {ex}")

            if df_final is not None:
                try:
                    if len(need_fetch_ranges) != 0:
                        save_pickle_safe(df_final, cache_path)
                except Exception:
                    pass
                
                if is_min:
                    result_df = df_final[(df_final['time'] >= req_start) & (df_final['time'] <= req_end)]
                else:
                    result_df = df_final[(df_final['time'].dt.date >= req_start.date()) & (df_final['time'].dt.date <= req_end.date())]
                if fields:
                    if "code" in result_df.columns:
                        result_df = result_df[fields_with_code]
                    else:
                        result_df = result_df[fields]
                if count is not None:
                    result_df = result_df.tail(count)
                all_dfs.append(result_df)
            else:
                raise RuntimeError(f"No data for {sec}")

        # # 合并逻辑（略，同前）
        # if len(all_dfs) == 1:
        #     return all_dfs[0]
        # else:
        #     # return pd.concat(all_dfs, axis=1, keys=securities) if panel else pd.concat(all_dfs, ignore_index=True)
        #     return pd.concat(all_dfs, ignore_index=True) # (tyb)TODO panel是个啥格式没搞懂，先暂时不考虑
    
        if fq != 'pre':
            if len(all_dfs) == 1:
                return all_dfs[0]
            else:
                # return pd.concat(all_dfs, axis=1, keys=securities) if panel else pd.concat(all_dfs, ignore_index=True)
                return pd.concat(all_dfs, ignore_index=True) # (tyb)TODO panel是个啥格式没搞懂，先暂时不考虑
        else:
            if len(all_dfs) == 1:
                adjusted = self._adjust_dataframe_result(all_dfs[0])
                return adjusted
            else:
                res_df = pd.concat(all_dfs, ignore_index=True)
                adjusted_dfs = self._adjust_dataframe_result(res_df)
                # return pd.concat(adjusted_dfs, axis=1, keys=securities) if panel else pd.concat(adjusted_dfs, ignore_index=True)
                return adjusted_dfs # (tyb)TODO panel是个啥格式没搞懂，先暂时不考虑

        # (tyb) TODO: 后续按照原作者的方式处理前复权的问题
        # (tyb) TODO: 再写一个函数，缓存获取五年的数据

    def _adjust_dataframe_result(self, data: Any) -> Any:
        if isinstance(data, pd.DataFrame):
            return self._adjust_dataframe(data)
        if hasattr(data, 'to_frame'):
            try:
                df = data.to_frame()
            except Exception:
                return data
            return self._adjust_dataframe(df)
        return data
    
    def _adjust_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        result_df = df.copy()
        cols = result_df.columns
        if isinstance(cols, pd.MultiIndex):
            top_levels = list(cols.get_level_values(0))
            if 'factor' not in top_levels:
                return result_df
            try:
                factor_block = result_df.xs('factor', axis=1, level=0)
            except Exception:
                return result_df
            ratio_df = self._compute_ratio_frame(factor_block)
            if ratio_df is None:
                return result_df
            for field in self._PRICE_SCALE_FIELDS:
                if field in top_levels:
                    try:
                        value_block = result_df.xs(field, axis=1, level=0)
                    except Exception:
                        continue
                    scaled = value_block.multiply(ratio_df, fill_value=0.0)
                    for code in scaled.columns:
                        result_df[(field, code)] = scaled[code]
            return result_df

        base_cols = list(result_df.columns)
        has_factor = 'factor' in base_cols
        if has_factor and 'code' in base_cols and 'time' in base_cols:
            return self._adjust_long_dataframe(result_df)
        if has_factor:
            ratio_series = self._compute_ratio_series(result_df['factor'])
            for field in self._PRICE_SCALE_FIELDS:
                if field in result_df.columns:
                    result_df[field] = result_df[field].multiply(ratio_series, fill_value=0.0)
            return result_df
        return result_df

    def _compute_ratio_frame(self, factor_df: pd.DataFrame) -> Optional[pd.DataFrame]:
        if factor_df is None or factor_df.empty:
            return None
        ratio_columns: Dict[str, pd.Series] = {}
        for col in factor_df.columns:
            ratio_columns[col] = self._compute_ratio_series(factor_df[col])
        ratio_df = pd.DataFrame(ratio_columns)
        ratio_df = ratio_df.reindex(factor_df.index)
        ratio_df.replace([float('inf'), float('-inf')], float('nan'), inplace=True)
        ratio_df = ratio_df.ffill().bfill()
        ratio_df.fillna(1.0, inplace=True)
        return ratio_df
    
    def _adjust_long_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        working = df.copy()
        if 'time' not in working.columns or 'code' not in working.columns or 'factor' not in working.columns:
            return working
        ratio = self._compute_ratio_series(working['factor'])
        for field in self._PRICE_SCALE_FIELDS:
            if field in working.columns:
                working[field] = working[field] * ratio
        return working
    
    def _compute_ratio_series(self, series: pd.Series) -> pd.Series:
        if series is None or series.empty:
            return pd.Series([], index=series.index if isinstance(series, pd.Series) else None, dtype=float)
        denom = pd.to_numeric(series, errors='coerce')
        denom.replace(0.0, float('nan'), inplace=True)
        ratio = 1.0 / denom
        ratio.replace([float('inf'), float('-inf')], float('nan'), inplace=True)
        ratio = ratio.ffill().bfill()
        ratio.fillna(1.0, inplace=True)
        return ratio

    # =================================================================
    # get_trade_days with full calendar caching
    # =================================================================
    def get_trade_days(
        self,
        start_date: Optional[Union[str, datetime.datetime]] = None,
        end_date: Optional[Union[str, datetime.datetime]] = None,
        count: Optional[int] = None
    ) -> List[datetime.datetime]:
        cache_path = os.path.join(CACHE_DIR, "trade_days", "full.pkl")
        full_list = load_pickle_safe(cache_path)

        if full_list is None:
            record_cache_miss()
            # 拉取一个大范围，比如 1990-2030
            full_list = self._fetch_full_trade_days()
            try:
                save_pickle_safe(full_list, cache_path)
            except Exception:
                pass
        else:
            record_cache_hit()

        # 转为 date
        date_list = [d.date() if isinstance(d, datetime.datetime) else d for d in full_list]

        # 过滤
        if start_date:
            start_date = parse_date(start_date)
            date_list = [d for d in date_list if d >= start_date]
        if end_date:
            end_date = parse_date(end_date)
            date_list = [d for d in date_list if d <= end_date]
        if count:
            date_list = date_list[-count:]

        return [datetime.datetime.combine(d, datetime.time()) for d in date_list]

    # =================================================================
    # 其他函数：精确缓存（快照性质）
    # =================================================================
    def get_all_securities(
        self,
        types: Union[str, List[str]] = 'stock',
        date: Optional[Union[str, datetime.datetime]] = None
    ) -> pd.DataFrame:
        use_date = to_date_str(date or datetime.date.today())
        cache_path = os.path.join(CACHE_DIR, "all_securities", f"{use_date}.pkl")
        cached = load_pickle_safe(cache_path)
        if cached is not None:
            record_cache_hit()
            return cached

        record_cache_miss()
        result = self._fetch_all_securities(types=types, date=use_date)
        try:
            save_pickle_safe(result, cache_path)
        except Exception:
            pass
        return result

    def get_index_stocks(
        self,
        index_symbol: str,
        date: Optional[Union[str, datetime.datetime]] = None
    ) -> List[str]:
        use_date = to_date_str(date or datetime.date.today())
        cache_path = os.path.join(CACHE_DIR, "index_stocks", f"{index_symbol}_{use_date}.pkl")
        cached = load_pickle_safe(cache_path)
        if cached is not None:
            record_cache_hit()
            return cached

        record_cache_miss()
        result = self._fetch_index_stocks(index_symbol=index_symbol, date=use_date)
        try:
            save_pickle_safe(result, cache_path)
        except Exception:
            pass
        return result

    def get_concept_stocks(
        self,
        concept_code: Union[str, List[str]],
        date: Optional[Union[str, datetime.datetime]] = None
    ) -> List[str]:
        codes = concept_code if isinstance(concept_code, str) else '_'.join(sorted(concept_code))
        use_date = to_date_str(date or datetime.date.today())
        cache_path = os.path.join(CACHE_DIR, "concept_stocks", f"{codes}_{use_date}.pkl")
        cached = load_pickle_safe(cache_path)
        if cached is not None:
            record_cache_hit()
            return cached

        record_cache_miss()
        result = self._fetch_concept_stocks(concept_code=concept_code, date=use_date)
        try:
            save_pickle_safe(result, cache_path)
        except Exception:
            pass
        return result

    def get_split_dividend(
        self,
        security: str,
        start_date: Optional[Union[str, datetime.datetime]] = None,
        end_date: Optional[Union[str, datetime.datetime]] = None
    ) -> List[Dict[str, Any]]:
        s_str = to_date_str(start_date or "1990-01-01")
        e_str = to_date_str(end_date or datetime.date.today())
        cache_path = os.path.join(CACHE_DIR, "split_dividend", f"{security}_{s_str}_{e_str}.pkl")
        cached = load_pickle_safe(cache_path)
        if cached is not None:
            record_cache_hit()
            return cached

        record_cache_miss()
        result = self._fetch_split_dividend(security=security, start_date=s_str, end_date=e_str)
        try:
            save_pickle_safe(result, cache_path)
        except Exception:
            pass
        return result

    # =================================================================
    # 远程调用接口（需你实现）
    # =================================================================
    def _fetch_price_from_remote(
        self,
        security: str,
        start_date: datetime.date,
        end_date: datetime.date,
        frequency: str,
        fields: Optional[List[str]],
        fq: str
    ) -> pd.DataFrame:
        # return jq.get_price(security, start_date=to_date_str(start_date), end_date=to_date_str(end_date),
        #                  frequency=frequency, fields=fields, fq=fq, panel=False)
        return jq.get_price(security, start_date=start_date, end_date=end_date,
                         frequency=frequency, fields=fields, fq=fq, panel=False)
                         
    def _fetch_full_trade_days(self) -> List[datetime.datetime]:
        return jq.get_trade_days('1990-01-01', '2030-12-31')

    def _fetch_all_securities(self, types, date) -> pd.DataFrame:
        return jq.get_all_securities(types, date)

    def _fetch_index_stocks(self, index_symbol, date) -> List[str]:
        return jq.get_index_stocks(index_symbol, date)

    def _fetch_concept_stocks(self, concept_code, date) -> List[str]:
        return jq.get_concept_stocks(concept_code, date)

    def _fetch_split_dividend(self, security: str,
                              start_date: Optional[Union[str, datetime.datetime, datetime.date]] = None,
                              end_date: Optional[Union[str, datetime.datetime, datetime.date]] = None
    ) -> List[Dict[str, Any]]:
        kwargs = {
            'security': security,
            'start_date': start_date,
            'end_date': end_date,
        }

        def _fetch(kw: Dict[str, Any]) -> List[Dict[str, Any]]:
            security_i = kw['security']
            sd = self._to_date(kw.get('start_date'))
            ed = self._to_date(kw.get('end_date'))
            if sd is None or ed is None:
                # Provider层要求明确日期
                return []
            sec_type = self._infer_security_type(security_i, ed)
            code_num = security_i.split('.')[0]
            events: List[Dict[str, Any]] = []
            try:
                if sec_type in ('fja', 'fjb'):
                    try:
                        table = finance.FUND_MF_DAILY_PROFIT
                    except Exception:
                        table = _FINANCE_TABLE_STUB
                    q = query(table).filter(
                        table.code == code_num,
                        table.day >= sd,
                        table.day <= ed
                    )
                    df = finance.run_query(q)
                    for _, row in df.iterrows():
                        daily_profit = float(row.get('daily_profit', 0.0) or 0.0)
                        events.append({
                            'security': security_i,
                            'date': row['day'],
                            'security_type': sec_type,
                            'scale_factor': 1.0,
                            'bonus_pre_tax': daily_profit / 10000.0,
                            'per_base': 1,
                        })
                elif sec_type in ('fund', 'etf', 'lof'):
                    try:
                        table = finance.FUND_DIVIDEND
                    except Exception:
                        table = _FINANCE_TABLE_STUB
                    q = query(table).filter(
                        table.code == code_num,
                        table.ex_date >= sd,
                        table.ex_date <= ed
                    )
                    df = finance.run_query(q)
                    for _, row in df.iterrows():
                        proportion = float(row.get('proportion', 0.0) or 0.0)
                        split_ratio = row.get('split_ratio', None)
                        try:
                            scale_factor = float(split_ratio) if split_ratio is not None else 1.0
                        except Exception:
                            scale_factor = 1.0
                        events.append({
                            'security': security_i,
                            'date': row.get('ex_date') or row.get('record_date'),
                            'security_type': sec_type,
                            'scale_factor': scale_factor,
                            # 聚宽事件口径：基金/ETF/LOF的 proportion 视为“每份派息”，按每1份为基数计算
                            'bonus_pre_tax': proportion,
                            'per_base': 1,
                        })
                else:
                    try:
                        table = finance.STK_XR_XD
                    except Exception:
                        table = _FINANCE_TABLE_STUB
                    q = query(table).filter(
                        table.code == security_i,
                        table.a_xr_date >= sd,
                        table.a_xr_date <= ed
                    )
                    df = finance.run_query(q)
                    for _, row in df.iterrows():
                        bonus_rmb = float(row.get('bonus_ratio_rmb', 0.0) or 0.0)
                        stock_paid = self._extract_ratio(
                            row,
                            ['dividend_ratio', 'stock_dividend_ratio'],
                            'dividend_number',
                        )
                        into_shares = self._extract_ratio(
                            row,
                            ['transfer_ratio', 'stock_transfer_ratio'],
                            'transfer_number',
                        )
                        per_base = 10
                        try:
                            scale_factor = 1.0 + (stock_paid + into_shares) / per_base
                        except Exception:
                            scale_factor = 1.0
                        events.append({
                            'security': security_i,
                            'date': row.get('a_xr_date') or row.get('a_bonus_date'),
                            'security_type': 'stock',
                            'scale_factor': scale_factor,
                            'bonus_pre_tax': bonus_rmb,
                            'per_base': per_base,
                        })
            except Exception:
                pass
            return events
        
        return _fetch(kwargs)

    # =================================================================
    # 缓存统计接口
    # =================================================================
    def get_cache_statistics(self) -> dict:
        return get_cache_stats()