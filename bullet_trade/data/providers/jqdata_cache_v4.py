import hashlib
import json
import os
import pickle
import datetime
import threading
import time
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
CACHE_DIR = "vibe_team_cache/jqdata_v4"
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
        '1m': '5m',  # 注意：1m 是分钟，不是月！
        'min': '5m',
        '5m': '5m',
        'minute': '5m',
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

def save_pickle_safe(obj, path, max_retries=3):
    for i in range(max_retries):
        try:
            with open(path, 'wb') as f:
                pickle.dump({'data': obj, 'timestamp': datetime.datetime.now()}, f)
            break
        except Exception as e:
            if i == max_retries - 1:
                raise e

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
        self._minute_price_cache_none = {}  # {month: DataFrame}
        
        # 合并后的DataFrame缓存 - LRU策略
        self._merged_df_cache = {}  # {cache_key: DataFrame}
        self._merged_df_access_times = {}  # {cache_key: last_access_time}
        self._merged_df_access_count = {}  # {cache_key: access_count}
        self._max_merged_cache_size = 10  # 最大缓存10个合并后的DataFrame
        
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
        
        # 延迟加载分钟线数据，第一次使用时再加载
        # self._load_minute_price()

        # # 加载分钟线数据(不复权的数据) 暂时用不上，而且还有bug似乎，暂时先不要用
        # self._load_minute_price_none()
        
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
        cache_path = os.path.join(CACHE_DIR, "factor_value", "factor_values.pkl")
        data = load_pickle_safe(cache_path)
        if data is None:
            raise FileNotFoundError(f"Factor values cache not found: {cache_path}")
        
        # 从合并的数据中提取factors字典
        # if isinstance(data, dict) and 'factors' in data:
        #     self._factor_values_cache = data['factors']
        # else:
        #     raise ValueError(f"Invalid factor values cache format: {cache_path}")
        if isinstance(data, dict):
            self._factor_values_cache = data
        
        logger.info(f"Loaded factor values cache: {list(self._factor_values_cache.keys())}")
    
    def _load_daily_price(self) -> None:
        """加载日线数据"""
        # 加载pre和none两种复权方式的数据
        for fq in ['pre', 'none']:
            cache_path = os.path.join(CACHE_DIR, "price", "daily", f"merged_daily_data_{fq}.pkl")
            data = load_pickle_safe(cache_path)
            if data is None:
                raise FileNotFoundError(f"Daily price cache not found: {cache_path}")
            self._daily_price_cache[fq] = data
            logger.info(f"Loaded daily price cache ({fq}): {data.shape}")
    
    def _load_minute_price(self) -> None:
        """加载分钟线数据（按月）"""
        minute_dir = os.path.join(CACHE_DIR, "price", "5m")
        if not os.path.exists(minute_dir):
            logger.warning(f"Minute price directory not found: {minute_dir}")
            return
        
        # 查找所有月度文件，支持both none和pre复权
        loaded_keys = set()  # 防止重复加载
        for filename in sorted(os.listdir(minute_dir)):
            # 只处理以merged_开头、以.pkl结尾，且不是备份文件的文件
            if (filename.startswith('merged_') and 
                filename.endswith('.pkl') and 
                not filename.endswith('.backup')):
                
                month_key = filename.replace('.pkl', '')
                
                # 防止重复加载
                if month_key in loaded_keys:
                    logger.warning(f"Duplicate key {month_key} found, skipping {filename}")
                    continue
                
                cache_path = os.path.join(minute_dir, filename)
                data = load_pickle_safe(cache_path)
                if data is not None:
                    self._minute_price_cache[month_key] = data
                    loaded_keys.add(month_key)
                    logger.info(f"Loaded minute price cache ({month_key}): {data.shape}")

    def _load_minute_price_for_month(self, year: int, month: int, fq: str = 'pre') -> None:
        """按月加载分钟线数据"""
        minute_dir = os.path.join(CACHE_DIR, "price", "5m")
        if not os.path.exists(minute_dir):
            logger.warning(f"Minute price directory not found: {minute_dir}")
            return

        if fq == 'pre':
            month_key = f"merged_{year}_{month:02d}_pre"
        else:
            month_key = f"merged_{year}_{month:02d}"
        
        # 如果已经在缓存中，直接返回
        if month_key in self._minute_price_cache:
            return
        
        cache_path = os.path.join(minute_dir, f"{month_key}.pkl")
        data = load_pickle_safe(cache_path)
        if data is not None:
            self._minute_price_cache[month_key] = data
            logger.info(f"Loaded minute price cache ({month_key}): {data.shape}")
        else:
            logger.warning(f"Minute price cache file not found: {cache_path}")

    def _ensure_minute_cache_for_date(self, target_date: datetime.date, fq: str = 'pre', max_months: int = 2) -> None:
        """确保指定日期及前2个月的分钟线缓存已加载"""
        current_date = target_date
        
        # 加载当前月份及前2个月
        for i in range(max_months):  # 包括当前月份，总共3个月
            year = current_date.year
            month = current_date.month
            self._load_minute_price_for_month(year, month, fq)
            
            # 向前推一个月
            if current_date.month == 1:
                current_date = datetime.date(current_date.year - 1, 12, 1)
            else:
                current_date = datetime.date(current_date.year, current_date.month - 1, 1)
        
        # 缓存管理：保持最近12个月的数据，移除更旧的数据
        self._manage_minute_cache_size(max_months=max_months)

    def _manage_minute_cache_size(self, max_months: int = 3) -> None:
        """管理分钟线缓存大小，只保留最近的月份数据"""
        if len(self._minute_price_cache) <= max_months:
            return
        
        # 提取所有月份键并按时间排序
        month_keys = list(self._minute_price_cache.keys())
        
        def extract_year_month(key: str) -> tuple:
            try:
                parts = key.replace('merged_', '').replace('_pre', '').split('_')
                return (int(parts[0]), int(parts[1]))
            except (ValueError, IndexError):
                return (0, 0)  # 无效键排在最前面，会被优先删除
        
        # 按年月排序，最旧的在前面
        month_keys.sort(key=extract_year_month)
        
        # 移除最旧的月份，保留最近的max_months个
        update_months_flag = False
        keys_to_remove = month_keys[:-max_months]
        for key in keys_to_remove:
            if key in self._minute_price_cache:
                del self._minute_price_cache[key]
                update_months_flag = True
                logger.info(f"Removed old minute price cache: {key}")
        
        # 如果移除了月份缓存，清理相关的合并缓存
        if update_months_flag:
            self._clear_invalid_merged_cache(keys_to_remove)

    def _generate_cache_key(self, months_needed: List[str], fq: str) -> str:
        """生成合并DataFrame的缓存键"""
        sorted_months = sorted(months_needed)
        return f"{fq}_{'_'.join(sorted_months)}"

    def _get_merged_dataframe(self, months_needed: List[str], fq: str) -> pd.DataFrame:
        """获取合并后的DataFrame，使用LRU缓存策略"""
        cache_key = self._generate_cache_key(months_needed, fq)
        current_time = time.time()
        
        # 检查缓存是否命中
        if cache_key in self._merged_df_cache:
            # 更新访问信息
            self._merged_df_access_times[cache_key] = current_time
            self._merged_df_access_count[cache_key] = self._merged_df_access_count.get(cache_key, 0) + 1
            logger.debug(f"Cache hit for merged DataFrame: {cache_key}")
            return self._merged_df_cache[cache_key]
        
        # 缓存未命中，需要创建合并的DataFrame
        cur_minute_cache = self._minute_price_cache if fq == 'pre' else self._minute_price_cache_none
        
        if len(months_needed) == 1:
            df = cur_minute_cache[months_needed[0]].copy()
        else:
            month_dfs = [cur_minute_cache[month_key] for month_key in months_needed]
            df = pd.concat(month_dfs, ignore_index=False, sort=False)
        
        # 存入缓存
        self._merged_df_cache[cache_key] = df
        self._merged_df_access_times[cache_key] = current_time
        self._merged_df_access_count[cache_key] = 1
        
        logger.info(f"Created and cached merged DataFrame: {cache_key}, shape: {df.shape}")
        
        # 检查缓存大小并进行LRU淘汰
        self._evict_least_used_merged_cache()
        
        return df

    def _evict_least_used_merged_cache(self) -> None:
        """基于LRU策略淘汰最不常用的合并DataFrame缓存"""
        if len(self._merged_df_cache) <= self._max_merged_cache_size:
            return
        
        current_time = time.time()
        
        # 计算每个缓存项的分数（综合考虑访问频率和时间）
        cache_scores = {}
        for cache_key in self._merged_df_cache.keys():
            last_access = self._merged_df_access_times.get(cache_key, 0)
            access_count = self._merged_df_access_count.get(cache_key, 0)
            
            # 时间衰减因子：越久没访问，分数越低
            time_decay = max(0, 1 - (current_time - last_access) / 3600)  # 1小时衰减
            
            # 综合分数：访问频率 × 时间衰减
            cache_scores[cache_key] = access_count * time_decay
        
        # 按分数排序，移除分数最低的
        sorted_items = sorted(cache_scores.items(), key=lambda x: x[1])
        items_to_remove = len(self._merged_df_cache) - self._max_merged_cache_size
        
        for i in range(items_to_remove):
            cache_key = sorted_items[i][0]
            df_shape = self._merged_df_cache[cache_key].shape
            
            del self._merged_df_cache[cache_key]
            del self._merged_df_access_times[cache_key]
            del self._merged_df_access_count[cache_key]
            
            logger.info(f"Evicted merged DataFrame cache: {cache_key}, shape: {df_shape}")

    def _clear_merged_cache(self) -> None:
        """清空所有合并DataFrame缓存"""
        cache_count = len(self._merged_df_cache)
        self._merged_df_cache.clear()
        self._merged_df_access_times.clear()
        self._merged_df_access_count.clear()
        if cache_count > 0:
            logger.info(f"Cleared {cache_count} merged DataFrame caches")

    def _clear_invalid_merged_cache(self, removed_month_keys: List[str]) -> None:
        """清理包含已删除月份键的合并缓存"""
        removed_set = set(removed_month_keys)
        invalid_cache_keys = []
        
        for cache_key in self._merged_df_cache.keys():
            # 解析缓存键中包含的月份
            parts = cache_key.split('_')[1:]  # 移除fq前缀
            if any(month_key in removed_set for month_key in parts):
                invalid_cache_keys.append(cache_key)
        
        # 删除无效的缓存
        for cache_key in invalid_cache_keys:
            if cache_key in self._merged_df_cache:
                del self._merged_df_cache[cache_key]
                del self._merged_df_access_times[cache_key]
                del self._merged_df_access_count[cache_key]
                logger.info(f"Removed invalid merged cache: {cache_key}")

    def _load_minute_price_none(self) -> None:
        """加载分钟线数据（按月，不复权）"""
        minute_dir = os.path.join(CACHE_DIR, "price", "5m")
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
                    self._minute_price_cache_none[month_key] = data
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
        security_is_list = True if isinstance(security, list) else False
        
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
                    days_back = max(1, count // 48 + 1)
                    start_date = end_date - datetime.timedelta(days=days_back)  # 多留一些余量，(tyb)TODO 更精确的计算
                    # 调整到最近的交易日
                    start_date_date = get_nearest_trade_day(trade_days, start_date.date(), direction='forward')
                    start_date = datetime.datetime.combine(start_date_date, datetime.time(9, 35))
                else:
                    start_date = datetime.datetime(2015, 1, 1, 9, 35)
            elif isinstance(start_date, str):
                try:
                    if ' ' in start_date:
                        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
                    else:
                        start_date = datetime.datetime.combine(datetime.datetime.strptime(start_date, '%Y-%m-%d').date(), datetime.time(9, 35))
                except ValueError:
                    start_date = datetime.datetime.combine(parse_date(start_date), datetime.time(9, 35))
            elif type(start_date) is datetime.date and not type(start_date) is datetime.datetime:
                start_date = datetime.datetime.combine(start_date, datetime.time(9, 35))
            
            # 延迟加载分钟线数据：根据 pre_factor_ref_date 确定需要加载的月份
            if pre_factor_ref_date is not None:
                ref_date = parse_date(pre_factor_ref_date) if isinstance(pre_factor_ref_date, (str, datetime.datetime)) else pre_factor_ref_date
                self._ensure_minute_cache_for_date(ref_date, fq)
            
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

        if fq != 'pre' or pre_factor_ref_date is None: # TODO 这里可能会有问题，加载指数的时候pre_factor_ref_date是None，但不确定指数是否有前复权因子
            return_price = self.transform_price_fields(return_price, security_is_list)
            # 删除factor列
            if 'factor' in return_price.columns and 'factor' not in actual_fields:
                return_price = return_price.drop(columns=['factor'])
            return return_price
        elif (not is_min and req_start >= pre_factor_ref_date) or \
             (is_min and start_date.date() >= pre_factor_ref_date):
            for field in self._PRICE_SCALE_FIELDS:
                if field in return_price.columns:
                    return_price[field] = return_price[field] / return_price['factor'].round(2)
            return_price = self.transform_price_fields(return_price, security_is_list)
            # 删除factor列
            if 'factor' in return_price.columns and 'factor' not in actual_fields:
                return_price = return_price.drop(columns=['factor'])
            return return_price
        else:
            adjusted_dfs = self._adjust_dataframe_result(return_price, pre_factor_ref_date)
            adjusted_dfs = self.transform_price_fields(adjusted_dfs, security_is_list)
            # 删除factor列
            if 'factor' in adjusted_dfs.columns and 'factor' not in actual_fields:
                adjusted_dfs = adjusted_dfs.drop(columns=['factor'])
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
        
        # 筛选日期范围（优化版本）
        # 优化1：预先转换时间列，避免重复转换
        time_dates = pd.to_datetime(df['time']).dt.date
        
        # 优化2：先按时间筛选（通常时间范围筛选后数据量会大幅减少）
        time_mask = (time_dates >= start_date) & (time_dates <= end_date)
        df_time_filtered = df[time_mask]
        
        # 优化3：在已经按时间筛选的数据上筛选股票（数据量更小）
        if len(securities) > 100:  # 如果股票数量很多，使用set查找更快
            securities_set = set(securities)
            code_mask = df_time_filtered['code'].map(lambda x: x in securities_set)
            df_filtered = df_time_filtered[code_mask].copy()
        else:
            df_filtered = df_time_filtered[df_time_filtered['code'].isin(securities)].copy()        
        
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
        # 如果分钟线缓存为空且是第一次使用，根据查询日期范围进行初始化加载
        if not self._minute_price_cache:
            # 使用查询的结束日期作为参考点进行初始化
            ref_date = end_datetime.date()
            self._ensure_minute_cache_for_date(ref_date, fq)
        
        # 注意：目前只实现了 pre 复权的延迟加载，none 复权仍使用原有逻辑
        cur_minute_cache = self._minute_price_cache if fq == 'pre' else self._minute_price_cache_none
        if not cur_minute_cache:
            if fq == 'pre':
                raise ValueError("Minute price cache not loaded for 'pre' mode")
            else:
                # none 复权方式暂时不支持延迟加载
                raise ValueError("Minute price cache for 'none' mode not loaded. Please ensure _load_minute_price_none() is called during initialization.")
        start_time = time.time()

        # 确定需要的月份和复权方式
        months_needed = []
        current_date = start_datetime.date()
        end_date = end_datetime.date()
        
        while current_date <= end_date:
            if fq == 'pre':
                month_key = f"merged_{current_date.strftime('%Y_%m')}_pre"
            else:
                month_key = f"merged_{current_date.strftime('%Y_%m')}"
            if month_key not in months_needed:
                months_needed.append(month_key)
            # 移到下个月
            if current_date.month == 12:
                current_date = datetime.date(current_date.year + 1, 1, 1)
            else:
                current_date = datetime.date(current_date.year, current_date.month + 1, 1)
        print("耗时检查月份：", time.time() - start_time)
        
        # 动态检查并加载缺失的月份缓存
        missing_months = [m for m in months_needed if m not in self._minute_price_cache]
        if missing_months:
            logger.info(f"Loading missing minute price cache months: {missing_months}")
            for month_key in missing_months:
                try:
                    # 解析月份key获取年月
                    parts = month_key.replace('merged_', '').replace('_pre', '').split('_')
                    if len(parts) >= 2:
                        year, month = int(parts[0]), int(parts[1])
                        self._load_minute_price_for_month(year, month, fq)
                except (ValueError, IndexError) as e:
                    logger.warning(f"Failed to parse month key {month_key}: {e}")
            
            # 再次检查是否还有缺失的月份
            still_missing = [m for m in months_needed if m not in self._minute_price_cache]
            if still_missing:
                raise ValueError(f"Minute price cache for months {still_missing} not found. Available: {list(self._minute_price_cache.keys())}")
            
            # 加载后进行缓存管理
            self._manage_minute_cache_size()
        
        print("耗时检查缺失月份：", time.time() - start_time)
        
        # 使用LRU缓存机制获取合并后的DataFrame
        df = self._get_merged_dataframe(months_needed, fq)
        print("耗时加载月份数据：", time.time() - start_time)
        
        if df.empty:
            raise ValueError(f"No minute data found for date range {start_datetime} to {end_datetime}")
        
        # # 筛选股票 TODO: 先注释掉，这个检查太耗时了，后面再看看怎么优化，假设就是数据都有
        # missing_securities = [s for s in securities if s not in df['code'].values]
        # if missing_securities:
        #     raise ValueError(f"Securities not found in minute cache: {missing_securities}")
        
        # 筛选日期范围（分钟线需要考虑精确时间）
        # 优化1：预先转换时间列，避免重复转换
        time_series = pd.to_datetime(df['time'])
        
        # 优化2：先按时间筛选（通常时间范围筛选后数据量会大幅减少）
        time_mask = (time_series >= start_datetime) & (time_series <= end_datetime)
        df_time_filtered = df[time_mask]
        
        # 优化3：在已经按时间筛选的数据上筛选股票（数据量更小）
        if len(securities) > 100:  # 如果股票数量很多，使用set查找更快
            securities_set = set(securities)
            code_mask = df_time_filtered['code'].map(lambda x: x in securities_set)
            df_filtered = df_time_filtered[code_mask].copy()
        else:
            df_filtered = df_time_filtered[df_time_filtered['code'].isin(securities)].copy()
        
        print("耗时筛选数据：", time.time() - start_time)
        
        if df_filtered.empty:
            raise ValueError(f"No minute data found for securities {securities} in date range {start_datetime} to {end_datetime}")
        
        # 优化5：智能排序，检查是否已经排序
        if not df_filtered['time'].is_monotonic_increasing:
            df_filtered = df_filtered.sort_values('time')
        print("耗时排序数据：", time.time() - start_time)
        
        # 优化6：应用count限制，减少不必要的操作
        if count is not None:
            # 使用groupby + tail，但避免reset_index如果不需要连续索引
            df_filtered = df_filtered.groupby('code', sort=False).tail(count)
            # 如果需要连续索引，在最后统一处理
            # df_filtered.reset_index(drop=True, inplace=True)
        print("耗时应用count限制：", time.time() - start_time)
        
        # 选择需要的字段（优化版本）
        available_fields = [f for f in fields if f in df_filtered.columns]
        if 'time' not in available_fields:
            available_fields.insert(0, 'time')
        if 'code' not in available_fields:
            available_fields.insert(0, 'code')
        if 'factor' in df_filtered.columns and 'factor' not in available_fields:
            available_fields.append('factor')

        # 如果'factor'不在缓存的字段中，从daily数据中获取
        if 'factor' in available_fields and 'factor' not in df_filtered.columns:
            # 获取对应的daily因子数据
            daily_df = self._daily_price_cache.get(fq)
            if daily_df is not None and 'factor' in daily_df.columns:
                # 构建一个多级索引的Series以便快速查找
                daily_factors = daily_df.set_index(['code', 'time'])['factor']
                
                # 映射因子值
                def map_factor(row):
                    key = (row['code'], row['time'].date())
                    return daily_factors.get(key, 1.0)  # 默认因子为1.0
                
                df_filtered['factor'] = df_filtered.apply(map_factor, axis=1)
            else:
                # 如果daily数据中也没有因子，移除该字段
                available_fields.remove('factor')
        
        # 最后统一处理索引重建，只在必要时执行
        result = df_filtered[available_fields]
        if not result.index.is_monotonic_increasing or result.index.duplicated().any():
            result = result.reset_index(drop=True)
        
        return result

    def transform_price_fields(self, df: pd.DataFrame, security_is_list: bool) -> pd.DataFrame:

        if security_is_list:
            return df
        else:
            df.set_index("time", inplace=True)
            df.index.name = None
            if "code" in df.columns:
                del df["code"]
            return df

    def _adjust_dataframe_result(self, data: Any, pre_factor_ref_date: Optional[Union[str, datetime.datetime]]) -> Any:
        if isinstance(data, pd.DataFrame):
            return self._adjust_dataframe(data, pre_factor_ref_date)
        if hasattr(data, 'to_frame'):
            try:
                df = data.to_frame()
            except Exception:
                return data
            return self._adjust_dataframe(df, pre_factor_ref_date)
        return data
    
    def _adjust_dataframe(self, df: pd.DataFrame, pre_factor_ref_date: Optional[Union[str, datetime.datetime]]) -> pd.DataFrame:
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
                # TODO: factor_block因子值为pre_factor_ref_date那一天的因子值，而不是每一天的因子值，要做的是将因子值调整为相对于pre_factor_ref_date的因子值并拓展维度到所有日期，暂时没有实现，这个没有测试数据，不太方便
                logger.error("没有实现完全！！！谨慎使用")
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
                        # 四舍无入到两位小数
                        scaled[code] = scaled[code].round(2)
                        result_df[(field, code)] = scaled[code]
            return result_df

        base_cols = list(result_df.columns)
        has_factor = 'factor' in base_cols
        if has_factor and 'code' in base_cols and 'time' in base_cols:
            return self._adjust_long_dataframe(result_df, pre_factor_ref_date)
        if has_factor:
            # 输入给_compute_ratio_series的因子值为pre_factor_ref_date那一天的因子值，而不是每一天的因子值，要做的是将因子值调整为相对于pre_factor_ref_date的因子值并拓展维度到所有日期
            unique_codes = result_df['code'].unique()
            ref_date_data = self._get_daily_price(unique_codes.tolist(), start_date=pre_factor_ref_date, end_date=pre_factor_ref_date, fields=['factor'], fq='pre', count=None)
            factor_map = dict(zip(ref_date_data['code'], ref_date_data['factor']))
            ref_factor = result_df['code'].map(factor_map).fillna(1.0)
            ratio_series = self._compute_ratio_series(ref_factor)
            for field in self._PRICE_SCALE_FIELDS:
                if field in result_df.columns:
                    result_df[field] = result_df[field].multiply(ratio_series, fill_value=0.0)
                    # 四舍无入到两位小数
                    result_df[field] = result_df[field].round(2)
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
    
    def _adjust_long_dataframe(self, df: pd.DataFrame, pre_factor_ref_date: Optional[Union[str, datetime.datetime]]) -> pd.DataFrame:
        working = df.copy()
        if 'time' not in working.columns or 'code' not in working.columns or 'factor' not in working.columns:
            return working
        # 输入给_compute_ratio_series的因子值为pre_factor_ref_date那一天的因子值，而不是每一天的因子值，要做的是将因子值调整为相对于pre_factor_ref_date的因子值并拓展维度到所有日期
        unique_codes = working['code'].unique()
        ref_date_data = self._get_daily_price(unique_codes.tolist(), start_date=pre_factor_ref_date, end_date=pre_factor_ref_date, fields=['factor'], fq='pre', count=None)
        factor_map = dict(zip(ref_date_data['code'], ref_date_data['factor']))
        ref_factor = working['code'].map(factor_map).fillna(1.0)
        ratio = self._compute_ratio_series(ref_factor)
        for field in self._PRICE_SCALE_FIELDS:
            if field in working.columns:
                working[field] = working[field] * ratio
                # 四舍五入到两位小数
                working[field] = working[field].round(2)
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
        elif 'security' in df.columns:
            filtered_df = df[df['security'] == security]
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

    def get_extras(
        self,
        info: str,
        security_list: List[str],
        start_date: Optional[Union[str, datetime.datetime]] = None,
        end_date: Optional[Union[str, datetime.datetime]] = None,
        df: bool = True,
        count: Optional[int] = None,
    ) -> Any:
        # TODO(tyb): 这个暂时没有缓存，后面需要加入缓存

        return jq.get_extras(
            info,
            security_list,
            start_date=start_date,
            end_date=end_date,
            df=df,
            count=count,
        )

    # =================================================================
    # get_fundamentals：如果缓存了则使用缓存，否则加入缓存
    # 暂时不要用，sql有点问题
    # =================================================================
    def get_fundamentals(
        self,
        query: Any,
        date: Optional[Union[str, datetime.datetime]] = None,
        statDate: Optional[Union[str, datetime.datetime]] = None,
        **kwargs
    ) -> pd.DataFrame:
        # 加入缓存机制，sql作为缓存key
        sql = jq.finance_service.get_fundamentals_sql(query, date, statDate)
        sql_hash_key = hashlib.sha1(sql.encode('utf-8')).hexdigest()
        cache_key = f"fundamentals_{sql_hash_key}"
        cache_path = os.path.join(CACHE_DIR, "fundamentals", f"{cache_key}.pkl")

        cached = load_pickle_safe(cache_path)
        if cached is not None:
            return cached
        # 调用原始接口获取数据
        result = jq.get_fundamentals(query, date=date, statDate=statDate, **kwargs)
        # 保存到缓存
        save_pickle_safe(result, cache_path)
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
                'minute_price': len(self._minute_price_cache),
                'merged_dataframes': len(self._merged_df_cache)
            }
        }
        
        if self._trade_days_cache is not None:
            stats['trade_days_count'] = len(self._trade_days_cache)
        
        if self._factor_values_cache is not None:
            stats['factor_count'] = len(self._factor_values_cache)
            stats['factor_names'] = list(self._factor_values_cache.keys())
        
        if self._split_dividend_cache is not None:
            stats['split_dividend_records'] = len(self._split_dividend_cache)
        
        # 合并缓存统计
        if self._merged_df_cache:
            stats['merged_cache_details'] = {
                'count': len(self._merged_df_cache),
                'keys': list(self._merged_df_cache.keys()),
                'total_memory_usage_mb': sum(
                    df.memory_usage(deep=True).sum() / 1024 / 1024 
                    for df in self._merged_df_cache.values()
                )
            }
        
        return stats