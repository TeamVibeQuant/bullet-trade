"""jqdata 兼容模块（vendored 入口）。

目标：
- 兼容聚宽策略的 `from third_party.bullet_trade.jqdata import *` / `from jqdata import *` 写法
- 同时让 VS Code/Pylance 能静态解析符号来源，实现 Cmd+Click 跳转到真实定义

实现方式：
- 避免使用 `from ... import *` 进行转发（静态分析工具难以追踪来源）
- 显式 re-export 需要的符号，并提供稳定的 `__all__`
"""

from __future__ import annotations

try:
    # 当以包子模块方式导入（third_party.bullet_trade.jqdata）时，优先走相对导入，便于静态分析
    from .bullet_trade.core.api import (
        # 全局对象
        g,
        log,
        send_msg,
        set_message_handler,
        # 订单相关
        order,
        order_target,
        order_value,
        order_target_value,
        cancel_order,
        cancel_all_orders,
        MarketOrderStyle,
        LimitOrderStyle,
        FixedSlippage,
        PriceRelatedSlippage,
        StepRelatedSlippage,
        OrderCost,
        PerTrade,
        # 调度器
        run_daily,
        run_weekly,
        run_monthly,
        unschedule_all,
        # 设置相关
        set_benchmark,
        set_option,
        set_slippage,
        set_order_cost,
        set_commission,
        set_universe,
        # 数据API
        get_price,
        live_get_price,
        attribute_history,
        get_current_data,
        get_trade_days,
        get_all_securities,
        get_index_stocks,
        get_split_dividend,
        set_data_provider,
        get_data_provider,
        get_factor_values,
        get_concept_stocks,
        get_concept,
        get_fundamentals,
        get_extras,
        # 研究文件读写
        read_file,
        write_file,
        # Tick 订阅与快照
        subscribe,
        unsubscribe,
        unsubscribe_all,
        get_current_tick,
        # 数据模型
        Context,
        Portfolio,
        SubPortfolio,
        Position,
        Order,
        Trade,
        OrderStatus,
        OrderStyle,
        SecurityUnitData,
    )

    from .bullet_trade.utils.strategy_helpers import (
        print_portfolio_info,
        prettytable_print_df,
    )
except Exception:  # pragma: no cover
    # 当以顶层模块方式导入（jqdata）时，相对导入不可用，回退到 canonical 包名
    from bullet_trade.core.api import (
        g,
        log,
        send_msg,
        set_message_handler,
        order,
        order_target,
        order_value,
        order_target_value,
        cancel_order,
        cancel_all_orders,
        MarketOrderStyle,
        LimitOrderStyle,
        FixedSlippage,
        PriceRelatedSlippage,
        StepRelatedSlippage,
        OrderCost,
        PerTrade,
        run_daily,
        run_weekly,
        run_monthly,
        unschedule_all,
        set_benchmark,
        set_option,
        set_slippage,
        set_order_cost,
        set_commission,
        set_universe,
        get_price,
        live_get_price,
        attribute_history,
        get_current_data,
        get_trade_days,
        get_all_securities,
        get_index_stocks,
        get_split_dividend,
        set_data_provider,
        get_data_provider,
        get_factor_values,
        get_concept_stocks,
        read_file,
        write_file,
        subscribe,
        unsubscribe,
        unsubscribe_all,
        get_current_tick,
        Context,
        Portfolio,
        SubPortfolio,
        Position,
        Order,
        Trade,
        OrderStatus,
        OrderStyle,
        SecurityUnitData,
    )

    from bullet_trade.utils.strategy_helpers import (
        print_portfolio_info,
        prettytable_print_df,
    )

# 常用模块：与聚宽环境一致，from jqdata import * 后可直接使用
import datetime as _datetime
import math as _math
import random as _random
import time as _time

import numpy as _np
import pandas as _pd

datetime = _datetime
math = _math
random = _random
time = _time
np = _np
pd = _pd

__all__ = [
    # 全局对象
    'g', 'log', 'send_msg', 'set_message_handler',

    # 订单相关
    'order', 'order_target', 'order_value', 'order_target_value', 'cancel_order', 'cancel_all_orders',
    'MarketOrderStyle', 'LimitOrderStyle',
    'FixedSlippage', 'PriceRelatedSlippage', 'StepRelatedSlippage',
    'OrderCost', 'PerTrade',

    # 调度器
    'run_daily', 'run_weekly', 'run_monthly', 'unschedule_all',

    # 设置相关
    'set_benchmark', 'set_option', 'set_slippage', 'set_order_cost', 'set_commission', 'set_universe',

    # 数据API
    'get_price', 'attribute_history',
    'live_get_price',
    'get_current_data',
    'get_trade_days', 'get_all_securities',
    'get_index_stocks',
    'get_split_dividend',
    'set_data_provider', 'get_data_provider',
    'get_factor_values', 'get_concept_stocks', 'get_concept', 'get_fundamentals', 'get_extras',

    # 研究文件读写
    'read_file', 'write_file',

    # Tick 订阅与快照
    'subscribe', 'unsubscribe', 'unsubscribe_all', 'get_current_tick',

    # 数据模型
    'Context', 'Portfolio', 'SubPortfolio', 'Position',
    'Order', 'Trade', 'OrderStatus', 'OrderStyle',
    'SecurityUnitData',

    # 策略辅助工具
    'print_portfolio_info',
    'prettytable_print_df',

    # 常用模块
    'datetime', 'math', 'random', 'time', 'np', 'pd',
]
