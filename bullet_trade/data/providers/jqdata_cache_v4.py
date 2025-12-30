import os
import pickle
import datetime
import threading
import logging
from typing import Optional, List, Dict, Any, Union, Tuple
from abc import ABC, abstractmethod
import pandas as pd

from .base import DataProvider
import jqdatasdk as jq

logger = logging.getLogger(__name__)

# ========================
# 配置 & 工具
# ========================
CACHE_DIR = "cache/jqdata_v4"
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
        'min': '1m',
        'minute': '1m',
        'month': 'monthly',
    }
    # 分钟线保留原样（如 '5m'）
    if freq.endswith('m') and freq[:-1].isdigit():
        return freq
    return mapping.get(freq, freq)

def is_minute_frequency(freq: str) -> bool:
    return ((freq.endswith('m') or freq.endswith('M')) and freq[:-1].isdigit()) or freq.startswith('min')

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
    # 支持带时间的格式
    if ' ' in d:
        return datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S').date()
    else:
        return datetime.datetime.strptime(d, '%Y-%m-%d').date()


def get_nearest_trade_day(trade_days_list, target, direction='forward'):
    for d in (trade_days_list if direction == 'forward' else reversed(trade_days_list)):
        if (direction == 'forward' and d >= target) or (direction == 'backward' and d <= target):
            return d
    return trade_days_list[0] if direction == 'forward' else trade_days_list[-1]

# ========================
# 缓存读写
# ========================
def load_pickle_safe(path):
    """安全加载pickle文件"""
    if not os.path.exists(path):
        return None
    try:
        with open(path, 'rb') as f:
            data = pickle.load(f)
        # 支持新旧格式
        if isinstance(data, dict) and 'data' in data:
            return data['data']
        return data
    except Exception as e:
        logger.error(f"Failed to load {path}: {e}")
        return None

# ========================
# 主类
# ========================
class JQDataCacheProvider(DataProvider):
    name: str = "jqdatacache_v4"
    _DEFAULT_PRICE_FIELDS: List[str] = ['open', 'close', 'high', 'low', 'volume', 'money']
    _PRICE_SCALE_FIELDS: set = {
        'open', 'close', 'high', 'low', 'avg', 'price', 'high_limit', 'low_limit', 'pre_close'
    }

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        super().__init__()
        self.config = config or {}
        
        # 内存缓存
        self._trade_days_cache = None
        self._all_securities_cache = {}
        self._index_stocks_cache = {}
        self._concept_stocks_cache = {}
        self._split_dividend_cache = None
        self._factor_values_cache = None
        self._daily_price_cache = {}  # {fq: DataFrame}
        self._minute_price_cache = {}  # {month: DataFrame}
        
        # 加载所有缓存到内存
        self._load_all_caches()
        
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

    def _load_all_caches(self) -> None:
        """加载所有缓存到内存"""
        logger.info("Loading all caches into memory...")
        
        # 加载交易日历
        self._load_trade_days()
        
        # 加载股票信息 - 保持原格式，按日期缓存
        # 暂时不预加载，因为日期太多
        
        # 加载分红配股数据
        self._load_split_dividend()
        
        # 加载因子数据
        self._load_factor_values()
        
        # 加载日线数据
        self._load_daily_price()
        
        # 加载分钟线数据
        self._load_minute_price()
        
        logger.info("All caches loaded successfully")
    
    def _load_trade_days(self) -> None:
        """加载交易日历"""
        cache_path = os.path.join(CACHE_DIR, "trade_days", "full.pkl")
        self._trade_days_cache = load_pickle_safe(cache_path)
        if self._trade_days_cache is None:
            raise FileNotFoundError(f"Trade days cache not found: {cache_path}")
        logger.info(f"Loaded trade days cache: {len(self._trade_days_cache)} days")
    
    def _load_split_dividend(self) -> None:
        """加载分红配股数据"""
        cache_path = os.path.join(CACHE_DIR, "split_dividend", "merged_split_dividend.pkl")
        self._split_dividend_cache = load_pickle_safe(cache_path)
        if self._split_dividend_cache is None:
            logger.warning(f"Split dividend cache not found: {cache_path}")
            self._split_dividend_cache = pd.DataFrame()
        else:
            logger.info(f"Loaded split dividend cache: {len(self._split_dividend_cache)} records")
    
    def _load_factor_values(self) -> None:
        """加载因子数据"""
        cache_path = os.path.join(CACHE_DIR, "factor_value", "merged_factor_values.pkl")
        data = load_pickle_safe(cache_path)
        if data is None:
            raise FileNotFoundError(f"Factor values cache not found: {cache_path}")
        
        # 从合并的数据中提取factors字典
        if isinstance(data, dict) and 'factors' in data:
            self._factor_values_cache = data['factors']
        else:
            raise ValueError(f"Invalid factor values cache format: {cache_path}")
        
        logger.info(f"Loaded factor values cache: {list(self._factor_values_cache.keys())}")
    
    def _load_daily_price(self) -> None:
        """加载日线数据"""
        # 加载pre和none两种复权方式的数据
        for fq in ['pre', 'none']:
            cache_path = os.path.join(CACHE_DIR, "price", "daily", f"merged_daily_data_{fq}_fq.pkl")
            data = load_pickle_safe(cache_path)
            if data is None:
                raise FileNotFoundError(f"Daily price cache not found: {cache_path}")
            self._daily_price_cache[fq] = data
            logger.info(f"Loaded daily price cache ({fq}): {data.shape}")
    
    def _load_minute_price(self) -> None:
        """加载分钟线数据（按月）"""
        minute_dir = os.path.join(CACHE_DIR, "price", "1m")
        if not os.path.exists(minute_dir):
            logger.warning(f"Minute price directory not found: {minute_dir}")
            return
        
        # 查找所有月度文件
        for filename in os.listdir(minute_dir):
            if filename.endswith('.pkl'):
                month_key = filename.replace('.pkl', '')
                cache_path = os.path.join(minute_dir, filename)
                data = load_pickle_safe(cache_path)
                if data is not None:
                    self._minute_price_cache[month_key] = data
                    logger.info(f"Loaded minute price cache ({month_key}): {data.shape}")

    # =================================================================
    # get_price - 完全基于内存缓存
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
        panel: bool = False,
        fill_paused: bool = True,
        pre_factor_ref_date: Optional[Union[str, datetime.datetime]] = None,
        prefer_engine: bool = False,
    ) -> pd.DataFrame:
        # 参数验证
        if end_date is not None and start_date is not None and count is not None:
            raise ValueError("When end_date is specified, only one of start_date or count should be provided.")
        if end_date is not None and start_date is None and count is None:
            raise ValueError("When end_date is specified, either start_date or count must be provided.")
        
        # 处理fields
        actual_fields = fields or self._DEFAULT_PRICE_FIELDS
        
        # 标准化频率
        freq = normalize_frequency(frequency)
        is_min = is_minute_frequency(freq)
        
        # 处理股票列表
        securities = [security] if isinstance(security, str) else security
        
        # 获取交易日历
        if self._trade_days_cache is None:
            raise RuntimeError("Trade days cache not loaded")
        
        trade_days = sorted([d.date() if isinstance(d, datetime.datetime) else d for d in self._trade_days_cache])
        
        # 处理日期范围
        if is_min:
            # 分钟线数据需要保留时间精度
            if end_date is None:
                end_date = datetime.datetime.now()
            elif isinstance(end_date, str):
                # 尝试解析为datetime，如果失败则解析为date再转换
                try:
                    if ' ' in end_date:
                        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
                    else:
                        end_date = datetime.datetime.combine(datetime.datetime.strptime(end_date, '%Y-%m-%d').date(), datetime.time(15, 0))
                except ValueError:
                    end_date = datetime.datetime.combine(parse_date(end_date), datetime.time(15, 0))
            elif type(end_date) is datetime.date and not type(end_date) is datetime.datetime:
                end_date = datetime.datetime.combine(end_date, datetime.time(15, 0))
            
            if start_date is None:
                if count is not None:
                    # # 对于分钟线，我们需要估算大概的开始时间
                    # # 每天约240个分钟（4小时），向前推count/240天
                    days_back = max(1, count // 240 + 1)
                    start_date = end_date - datetime.timedelta(days=days_back)  # 多留一些余量，(tyb)TODO 更精确的计算
                    # 调整到最近的交易日
                    start_date_date = get_nearest_trade_day(trade_days, start_date.date(), direction='forward')
                    start_date = datetime.datetime.combine(start_date_date, datetime.time(9, 31))
                else:
                    start_date = datetime.datetime(2015, 1, 1, 9, 31)
            elif isinstance(start_date, str):
                try:
                    if ' ' in start_date:
                        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
                    else:
                        start_date = datetime.datetime.combine(datetime.datetime.strptime(start_date, '%Y-%m-%d').date(), datetime.time(9, 31))
                except ValueError:
                    start_date = datetime.datetime.combine(parse_date(start_date), datetime.time(9, 31))
            elif type(start_date) is datetime.date and not type(start_date) is datetime.datetime:
                start_date = datetime.datetime.combine(start_date, datetime.time(9, 31))
            
            return_price = self._get_minute_price(
                securities, start_date, end_date, actual_fields, fq, count
            )
        else:
            # 日线数据保持原逻辑
            if end_date is None:
                end_date = datetime.date.today()
            else:
                end_date = parse_date(end_date)
            req_end = get_nearest_trade_day(trade_days, end_date, direction='backward')
            
            if start_date is None:
                if count is not None:
                    trade_days_cur = [d for d in trade_days if d <= req_end]
                    if len(trade_days_cur) >= count:
                        start_date = trade_days_cur[-count]
                    else:
                        start_date = trade_days_cur[0] if trade_days_cur else req_end
                else:
                    start_date = datetime.date(2015, 1, 1)
            else:
                start_date = parse_date(start_date)
            req_start = get_nearest_trade_day(trade_days, start_date, direction='forward')
            
            return_price = self._get_daily_price(
                securities, req_start, req_end, actual_fields, fq, count
            )

        if fq != 'pre':
            return return_price
        else:
            adjusted_dfs = self._adjust_dataframe_result(return_price)
            return adjusted_dfs
    
    def _get_daily_price(
        self,
        securities: List[str],
        start_date: datetime.date,
        end_date: datetime.date,
        fields: List[str],
        fq: str,
        count: Optional[int]
    ) -> pd.DataFrame:
        """获取日线数据"""
        if fq not in self._daily_price_cache:
            raise ValueError(f"Daily price cache for fq='{fq}' not found. Available: {list(self._daily_price_cache.keys())}")
        
        df = self._daily_price_cache[fq]
        
        # 筛选股票
        missing_securities = [s for s in securities if s not in df['code'].values]
        if missing_securities:
            raise ValueError(f"Securities not found in cache: {missing_securities}")
        
        # 筛选日期范围
        df_filtered = df[
            (df['code'].isin(securities)) &
            (pd.to_datetime(df['time']).dt.date >= start_date) &
            (pd.to_datetime(df['time']).dt.date <= end_date)
        ].copy()
        
        if df_filtered.empty:
            raise ValueError(f"No data found for securities {securities} in date range {start_date} to {end_date}")
        
        # 按时间排序
        df_filtered = df_filtered.sort_values('time')
        
        # 应用count限制（优化版本）
        if count is not None:
            # 使用groupby + tail的矢量化操作，比for循环快很多
            df_filtered = df_filtered.groupby('code').tail(count).reset_index(drop=True)
        
        # 选择需要的字段
        available_fields = [f for f in fields if f in df_filtered.columns]
        if 'time' not in available_fields:
            available_fields = ['time'] + available_fields
        if 'code' not in available_fields:
            available_fields = ['code'] + available_fields
        if 'factor' in df_filtered.columns and 'factor' not in available_fields:
            available_fields.append('factor')
        
        return df_filtered[available_fields].reset_index(drop=True)
    
    def _get_minute_price(
        self,
        securities: List[str],
        start_datetime: datetime.datetime,
        end_datetime: datetime.datetime,
        fields: List[str],
        fq: str,
        count: Optional[int]
    ) -> pd.DataFrame:
        """获取分钟线数据"""
        if not self._minute_price_cache:
            raise ValueError("Minute price cache not loaded")
        
        # 确定需要的月份
        months_needed = []
        current_date = start_datetime.date()
        end_date = end_datetime.date()
        
        while current_date <= end_date:
            month_key = f"merged_{current_date.strftime('%Y_%m')}"
            if month_key not in months_needed:
                months_needed.append(month_key)
            # 移到下个月
            if current_date.month == 12:
                current_date = datetime.date(current_date.year + 1, 1, 1)
            else:
                current_date = datetime.date(current_date.year, current_date.month + 1, 1)
        
        # 检查缓存是否存在
        missing_months = [m for m in months_needed if m not in self._minute_price_cache]
        if missing_months:
            raise ValueError(f"Minute price cache for months {missing_months} not found. Available: {list(self._minute_price_cache.keys())}")
        
        # 合并所有需要的月份数据
        month_dfs = []
        for month_key in months_needed:
            month_df = self._minute_price_cache[month_key]
            month_dfs.append(month_df)
        
        if not month_dfs:
            raise ValueError(f"No minute data found for date range {start_datetime} to {end_datetime}")
        
        df = pd.concat(month_dfs, ignore_index=True)
        
        # # 筛选股票 TODO: 先注释掉，这个检查太耗时了，后面再看看怎么优化，假设就是数据都有
        # missing_securities = [s for s in securities if s not in df['code'].values]
        # if missing_securities:
        #     raise ValueError(f"Securities not found in minute cache: {missing_securities}")
        
        # 筛选日期范围（分钟线需要考虑精确时间）
        df_filtered = df[
            (df['code'].isin(securities)) &
            (pd.to_datetime(df['time']) >= start_datetime) &
            (pd.to_datetime(df['time']) <= end_datetime)
        ].copy()
        
        if df_filtered.empty:
            raise ValueError(f"No minute data found for securities {securities} in date range {start_datetime} to {end_datetime}")
        
        # 按时间排序
        df_filtered = df_filtered.sort_values('time')
        
        # 应用count限制 - 对每个股票分别应用（优化版本）
        if count is not None:
            # 使用groupby + tail的矢量化操作，比for循环快很多
            df_filtered = df_filtered.groupby('code').tail(count).reset_index(drop=True)
            # # 重新按时间排序
            # if not df_filtered.empty:
            #     df_filtered = df_filtered.sort_values('time')
        
        # 选择需要的字段
        available_fields = [f for f in fields if f in df_filtered.columns]
        if 'time' not in available_fields:
            available_fields = ['time'] + available_fields
        if 'code' not in available_fields:
            available_fields = ['code'] + available_fields
        if 'factor' in df_filtered.columns and 'factor' not in available_fields:
            available_fields.append('factor')
        
        return df_filtered[available_fields].reset_index(drop=True)

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
    # get_trade_days - 基于内存缓存
    # =================================================================
    def get_trade_days(
        self,
        start_date: Optional[Union[str, datetime.datetime]] = None,
        end_date: Optional[Union[str, datetime.datetime]] = None,
        count: Optional[int] = None
    ) -> List[datetime.datetime]:
        if self._trade_days_cache is None:
            raise RuntimeError("Trade days cache not loaded")

        # 转为 date
        date_list = [d.date() if isinstance(d, datetime.datetime) else d for d in self._trade_days_cache]

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
    # 其他函数：基于内存缓存
    # =================================================================
    def get_all_securities(
        self,
        types: Union[str, List[str]] = 'stock',
        date: Optional[Union[str, datetime.datetime]] = None
    ) -> pd.DataFrame:
        # 保持原格式，按需要加载
        use_date = date.strftime('%Y-%m-%d') if isinstance(date, datetime.date) else str(date or datetime.date.today())
        cache_path = os.path.join(CACHE_DIR, "all_securities", f"{use_date}.pkl")
        
        cached = load_pickle_safe(cache_path)
        if cached is None:
            raise FileNotFoundError(f"All securities cache not found for date {use_date}: {cache_path}")
        
        return cached

    def get_index_stocks(
        self,
        index_symbol: str,
        date: Optional[Union[str, datetime.datetime]] = None
    ) -> List[str]:
        use_date = date.strftime('%Y-%m-%d') if isinstance(date, datetime.date) else str(date or datetime.date.today())
        cache_path = os.path.join(CACHE_DIR, "index_stocks", f"{index_symbol}_{use_date}.pkl")
        
        cached = load_pickle_safe(cache_path)
        if cached is None:
            raise FileNotFoundError(f"Index stocks cache not found for {index_symbol} on {use_date}: {cache_path}")
        
        return cached

    def get_concept_stocks(
        self,
        concept_code: Union[str, List[str]],
        date: Optional[Union[str, datetime.datetime]] = None
    ) -> List[str]:
        codes = concept_code if isinstance(concept_code, str) else '_'.join(sorted(concept_code))
        use_date = date.strftime('%Y-%m-%d') if isinstance(date, datetime.date) else str(date or datetime.date.today())
        cache_path = os.path.join(CACHE_DIR, "concept_stocks", f"{codes}_{use_date}.pkl")
        
        cached = load_pickle_safe(cache_path)
        if cached is None:
            raise FileNotFoundError(f"Concept stocks cache not found for {codes} on {use_date}: {cache_path}")
        
        return cached

    def get_split_dividend(
        self,
        security: str,
        start_date: Optional[Union[str, datetime.datetime]] = None,
        end_date: Optional[Union[str, datetime.datetime]] = None
    ) -> List[Dict[str, Any]]:
        if self._split_dividend_cache is None or self._split_dividend_cache.empty:
            return []
        
        # 处理日期范围
        req_start = parse_date(start_date or "1990-01-01")
        req_end = parse_date(end_date or datetime.date.today())
        
        # 筛选数据
        df = self._split_dividend_cache
        
        # 检查是否有code列，如果没有就使用索引
        if 'code' in df.columns:
            filtered_df = df[df['code'] == security]
        else:
            # 假设索引就是股票代码（根据合并脚本的逻辑）
            logger.warning("No 'code' column in split_dividend cache, assuming index is stock code")
            return []
        
        if filtered_df.empty:
            return []
        
        # 转换为字典列表格式
        result = []
        for _, row in filtered_df.iterrows():
            record_dict = row.to_dict()
            # 检查日期范围
            if 'date' in record_dict:
                record_date = parse_date(record_dict['date'])
                if req_start <= record_date <= req_end:
                    result.append(record_dict)
        
        return result

    def get_factor_values(
        self,
        securities: Union[str, List[str]],
        factors: Union[str, List[str]],
        start_date: Optional[Union[str, datetime.datetime]] = None,
        end_date: Optional[Union[str, datetime.datetime]] = None,
        count: Optional[int] = None
    ) -> Dict[str, pd.DataFrame]:
        # 参数验证
        if end_date is not None and start_date is not None and count is not None:
            raise ValueError("When end_date is specified, only one of start_date or count should be provided.")
        if end_date is not None and start_date is None and count is None:
            raise ValueError("When end_date is specified, either start_date or count must be provided.")

        # 标准化输入
        sec_list = [securities] if isinstance(securities, str) else securities
        factor_list = [factors] if isinstance(factors, str) else factors
        
        # 检查因子缓存是否加载
        if self._factor_values_cache is None:
            raise RuntimeError("Factor values cache not loaded")
        
        # 处理日期范围
        if self._trade_days_cache is None:
            raise RuntimeError("Trade days cache not loaded")
        
        trade_days_list = sorted([d.date() if isinstance(d, datetime.datetime) else d for d in self._trade_days_cache])

        if end_date is None:
            end_date = datetime.date.today()
        else:
            end_date = parse_date(end_date)
        req_end = get_nearest_trade_day(trade_days_list, end_date, direction='backward')
            
        if start_date is None:
            if count is not None:
                # 根据count估算开始日期
                trade_days_cur = [d for d in trade_days_list if d <= req_end]
                if len(trade_days_cur) >= count:
                    start_date = trade_days_cur[-count]
                else:
                    start_date = trade_days_cur[0] if trade_days_cur else req_end
            else:
                start_date = datetime.date(2015, 1, 1)
        else:
            start_date = parse_date(start_date)
        req_start = get_nearest_trade_day(trade_days_list, start_date, direction='forward')

        # 如果start_date > end_date，直接返回空结果
        if req_start > req_end:
            return {factor: pd.DataFrame() for factor in factor_list}
        
        result = {}
        
        for factor in factor_list:
            if factor not in self._factor_values_cache:
                raise ValueError(f"Factor '{factor}' not found in cache. Available factors: {list(self._factor_values_cache.keys())}")
            
            factor_df = self._factor_values_cache[factor]
            
            # 检查所需股票是否存在
            missing_stocks = [sec for sec in sec_list if sec not in factor_df.columns]
            if missing_stocks:
                logger.warning(f"Securities not found in factor '{factor}' cache: {missing_stocks}")
            
            # 只保留存在的股票
            available_stocks = [sec for sec in sec_list if sec in factor_df.columns]
            
            if not available_stocks:
                result[factor] = pd.DataFrame()
                continue
            
            # 筛选日期范围
            factor_dates = pd.to_datetime(factor_df.index).date if hasattr(factor_df.index, 'date') else [pd.to_datetime(idx).date() for idx in factor_df.index]
            
            filtered_df = factor_df.loc[
                (pd.to_datetime(factor_df.index).date >= req_start) & 
                (pd.to_datetime(factor_df.index).date <= req_end),
                available_stocks
            ]
            
            # 应用count限制
            if count is not None:
                filtered_df = filtered_df.tail(count)
            
            result[factor] = filtered_df
        
        return result

    # =================================================================
    # 缓存统计接口
    # =================================================================
    def get_cache_statistics(self) -> dict:
        """返回缓存统计信息"""
        stats = {
            'provider_type': 'memory_cache_v4',
            'cache_dir': CACHE_DIR,
            'loaded_caches': {
                'trade_days': self._trade_days_cache is not None and len(self._trade_days_cache) > 0,
                'split_dividend': self._split_dividend_cache is not None and len(self._split_dividend_cache) > 0,
                'factor_values': self._factor_values_cache is not None and len(self._factor_values_cache) > 0,
                'daily_price': len(self._daily_price_cache),
                'minute_price': len(self._minute_price_cache)
            }
        }
        
        if self._trade_days_cache is not None:
            stats['trade_days_count'] = len(self._trade_days_cache)
        
        if self._factor_values_cache is not None:
            stats['factor_count'] = len(self._factor_values_cache)
            stats['factor_names'] = list(self._factor_values_cache.keys())
        
        if self._split_dividend_cache is not None:
            stats['split_dividend_records'] = len(self._split_dividend_cache)
        
        return stats