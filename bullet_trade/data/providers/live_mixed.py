"""Mixed data provider.

用途
----
实盘时常见需求：
- 行情/最新价来自本地 miniQMT（xtquant.xtdata），速度快且可订阅 tick；
- 其余用于选股/因子/历史回测一致性的接口仍使用 JQData。

本 provider 的约定：
- DataProvider 抽象接口方法（get_price/get_trade_days/…）默认走 JQData，保证历史/研究口径一致；
- 额外提供 live_get_price() / tick 订阅相关接口，强制走 miniQMT。

注意：DataProvider 基类并未定义 live_get_price()，该方法供上层 data.api.live_get_price() 透传使用。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from .base import DataProvider
from .jqdata import JQDataProvider
from .miniqmt import MiniQMTProvider


class LiveMixedProvider(DataProvider):
    """JQData(历史/研究) + MiniQMT(实盘行情) 组合 Provider。"""

    name: str = "live_mixed"
    # 实盘必须能拿到实时价（否则宁愿报错也不要回落到历史数据）
    requires_live_data: bool = True

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        jq_provider: Optional[DataProvider] = None,
        live_provider: Optional[DataProvider] = None,
    ) -> None:
        self.config = config or {}

        # 允许注入，便于测试或外部自定义
        self._jq: DataProvider
        self._live: DataProvider

        if jq_provider is not None:
            self._jq = jq_provider
        else:
            jq_cfg = dict(self.config.get("jqdata", {}) or {})
            self._jq = JQDataProvider(jq_cfg)

        if live_provider is not None:
            self._live = live_provider
        else:
            qmt_cfg = dict(self.config.get("qmt", {}) or self.config.get("miniqmt", {}) or {})
            # 强制 live 模式，避免默认 backtest 行为带来自动下载等副作用
            qmt_cfg.setdefault("mode", "live")
            self._live = MiniQMTProvider(qmt_cfg)

    def auth(
        self,
        user: Optional[str] = None,
        pwd: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
    ) -> None:
        # JQData 可能需要认证；miniQMT 一般不需要，但保留调用
        try:
            self._jq.auth(user=user, pwd=pwd, host=host, port=port)
        except Exception:
            # 上层会决定是否容忍认证失败
            raise
        try:
            self._live.auth()
        except Exception:
            # miniQMT auth 可忽略
            pass

    # ------------------------ 历史/研究接口：走 JQData ------------------------
    def get_price(
        self,
        security: Union[str, List[str]],
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        frequency: str = "daily",
        fields: Optional[List[str]] = None,
        skip_paused: bool = False,
        fq: str = "pre",
        count: Optional[int] = None,
        panel: bool = True,
        fill_paused: bool = True,
        pre_factor_ref_date: Optional[Union[str, datetime]] = None,
        prefer_engine: bool = False,
    ) -> pd.DataFrame:
        return self._jq.get_price(
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
            pre_factor_ref_date=pre_factor_ref_date,
            prefer_engine=prefer_engine,
        )

    def get_trade_days(
        self,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        count: Optional[int] = None,
    ) -> List[datetime]:
        return self._jq.get_trade_days(start_date=start_date, end_date=end_date, count=count)

    def get_all_securities(
        self,
        types: Union[str, List[str]] = "stock",
        date: Optional[Union[str, datetime]] = None,
    ) -> pd.DataFrame:
        return self._jq.get_all_securities(types=types, date=date)

    def get_index_stocks(
        self,
        index_symbol: str,
        date: Optional[Union[str, datetime]] = None,
    ) -> List[str]:
        return self._jq.get_index_stocks(index_symbol=index_symbol, date=date)

    def get_split_dividend(
        self,
        security: str,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
    ) -> List[Dict[str, Any]]:
        return self._jq.get_split_dividend(security=security, start_date=start_date, end_date=end_date)

    def get_security_info(self, security: str) -> Dict[str, Any]:
        return self._jq.get_security_info(security)

    # ------------------------ 实盘行情接口：走 miniQMT ------------------------
    def live_get_price(
        self,
        security: Union[str, List[str]],
        *,
        frequency: str = "1m",
        fields: Optional[List[str]] = None,
        fq: str = "none",
        count: int = 1,
        panel: bool = True,
    ) -> pd.DataFrame:
        """获取实盘最新行情（默认取 1m 最新一根）。"""
        return self._live.get_price(
            security=security,
            start_date=None,
            end_date=None,
            frequency=frequency,
            fields=fields,
            skip_paused=False,
            fq=fq,
            count=count,
            panel=panel,
            fill_paused=True,
            pre_factor_ref_date=None,
            prefer_engine=False,
        )

    def get_current_tick(self, security: str) -> Optional[Dict[str, Any]]:
        return self._live.get_current_tick(security)

    def subscribe_ticks(self, symbols: List[str]) -> None:
        return self._live.subscribe_ticks(symbols)

    def subscribe_markets(self, markets: List[str]) -> None:
        return self._live.subscribe_markets(markets)

    def unsubscribe_ticks(self, symbols: Optional[List[str]] = None) -> None:
        return self._live.unsubscribe_ticks(symbols)

    def unsubscribe_markets(self, markets: Optional[List[str]] = None) -> None:
        return self._live.unsubscribe_markets(markets)
