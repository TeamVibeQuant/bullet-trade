# 克隆自聚宽文章：https://www.joinquant.com/post/64640
# 标题：基于概念动量的小市值量化策略：选股逻辑与风控体系全解析
# 作者：阿萨德szx


try:
    from dotenv import load_dotenv
    import os
    from jqdatasdk import *
    from bullet_trade.core import OrderStatus
    load_dotenv()
    auth(os.getenv("JQ_ACCOUNT"), os.getenv("JQ_PASSWORD"))
except:
    log.info('Online env')

#导入函数库
import copy
import time as py_time
from jqdata import *
import numpy as np
import pandas as pd
from datetime import time, timedelta, date

# prifiling decorator
def profile(func):
    def wrapper(*args, **kwargs):
        start_time = py_time.time()
        result = func(*args, **kwargs)
        end_time = py_time.time()
        elapsed_time = end_time - start_time
        log.debug(f"[Profiling] Function '{func.__name__}' executed in {elapsed_time:.4f} seconds")
        return result
    return wrapper


#初始化函数 
def initialize(context):
    g.signal = ''
    # 开启防未来函数
    set_option('avoid_future_data', True)
    # 成交量设置
    set_option('order_volume_ratio', 0.02)
    # 设定基准
    set_benchmark('399101.XSHE')
    # 用真实价格交易
    set_option('use_real_price', True)
    # 将滑点设置为0
    set_slippage(PriceRelatedSlippage(0.004))
    set_option('match_with_order_book', True)  # 模拟盘启用盘口撮合
    # 设置交易成本万分之三，不同滑点影响可在归因分析中查看
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=2.5/10000, close_commission=2.5/10000, close_today_commission=0, min_commission=5),type='stock')
    # 过滤order中低于error级别的日志
    log.set_level('order', 'error')
    log.set_level('system', 'error')
    log.set_level('strategy', 'debug')
    #初始化全局变量 bool
    g.no_trading_today_signal = False  # 是否为可交易日
    g.pass_april = True  # 是否四月空仓
    g.run_stoploss = True  # 是否进行止损
    #全局变量list
    g.hold_list = [] #当前持仓的全部股票    
    g.yesterday_HL_list = [] #记录持仓中昨日涨停的股票
    g.target_list = []
    g.not_buy_again = []
    #全局变量
    g.stock_num = 6  #买入股票数量
    g.up_price = 100  # 设置股票单价 
    g.reason_to_sell = ''
    g.stoploss_strategy = 3  # 1为止损线止损，2为市场趋势止损, 3为联合1、2策略
    g.stoploss_limit = 0.91  # 止损线
    g.stoploss_market = 0.95  # 市场趋势止损参数

    g.concepts_df = None
    g.concept_stocks = {}
    
    g.HV_control = False #新增，Ture是日频判断是否放量，False则不然
    g.HV_duration = 120 #HV_control用，周期可以是240-120-60，默认比例是0.9
    g.HV_ratio = 0.9    #HV_control用
    g.stockL = []
    g.no_trading_buy = ['600036.XSHG','518880.XSHG','600900.XSHG']  # 空仓月份持有 
    g.no_trading_hold_signal = False
    # 设置交易运行时间
    run_daily(prepare_stock_list, '9:05')
    run_weekly(weekly_adjustment, 2-1, '10:30')
    run_daily(sell_stocks, time='10:00') # 止损函数
    run_daily(trade_afternoon, time='14:25') #检查持仓中的涨停股是否需要卖出
    run_daily(trade_afternoon, time='14:55') #检查持仓中的涨停股是否需要卖出
    run_daily(close_account, '14:50')
    # run_weekly(print_position_info, 5, time='15:10')


#1-1 准备股票池
@profile
def prepare_stock_list(context):
    print("---------------------prepare_stock_list---------------------")
    #获取已持有列表
    g.hold_list= []
    for position in list(context.portfolio.positions.values()):
        stock = position.security
        g.hold_list.append(stock)
    #获取昨日涨停列表
    if g.hold_list != []:
        df = get_price(g.hold_list, end_date=context.previous_date, frequency='daily', fields=['close','high_limit','low_limit'], count=1, panel=False, fill_paused=False)
        df = df[df['close'] == df['high_limit']]
        g.yesterday_HL_list = list(df.code)
    else:
        g.yesterday_HL_list = []
    #判断今天是否为账户资金再平衡的日期
    g.no_trading_today_signal = today_is_between(context)


#1-2 选股模块
@profile
def get_stock_list(context):
    MKT_index = '399101.XSHE'
    initial_list = get_index_stocks(MKT_index, date=context.current_dt)

    # 获取股票的最新价格、涨跌停、是否停牌等信息
    stock_info_all = get_price(initial_list, end_date=context.current_dt, frequency='1m', 
                               fields=['close', 'high_limit','low_limit', 'paused'], count=1, panel=False)    
    stock_info_all.index = stock_info_all['code']
    
    # 获取是否ST信息
    stock_st_info_all = get_extras('is_st', initial_list, end_date=context.current_dt, df=True, count=1)

    # 获取股票的上市退市时间信息
    stock_start_end_info_all = get_all_securities(types=['stock'], date=context.current_dt)
    start_dates_all = stock_start_end_info_all['start_date']
    end_dates_all = stock_start_end_info_all['end_date']

    positions = list(context.portfolio.positions.keys())

    filtered_list = []
    for stock in initial_list:
        last_close = stock_info_all.close[stock]
        paused = stock_info_all.paused[stock]
        high_limit = stock_info_all.high_limit[stock]
        low_limit = stock_info_all.low_limit[stock]
        
        is_stock_st = stock_st_info_all[stock].iloc[0]

        end_date = end_dates_all.get(stock, None)
        start_date = start_dates_all.get(stock, None)
        is_stock_end = end_date is not None and end_date.date() <= context.current_dt.date()

        if paused:  # 停牌
            continue

        if is_stock_st:  # ST
            continue

        if is_stock_end:  # 退市
            continue

        if stock.startswith('30') or stock.startswith('68') or stock.startswith('8') or stock.startswith('4'):  # 市场类型  
            continue
        
        if not (stock in positions or last_close < high_limit):  # 涨停
            continue
        
        if not (stock in positions or last_close > low_limit):  # 跌停
            continue
        
        # 次新股过滤
        if context.previous_date - start_date.date() < timedelta(days=375):
            continue
        
        filtered_list.append(stock)

    q = query(valuation.code).filter(valuation.code.in_(filtered_list)).order_by(valuation.circulating_market_cap.asc()).limit(200)
    filtered_list = list(get_fundamentals(q, date=context.previous_date).code)
    q = query(valuation.code, indicator.eps).filter(valuation.code.in_(filtered_list)).order_by(valuation.market_cap.asc())
    df = get_fundamentals(q, date=context.previous_date)
    stock_list = list(df.code)
    stock_list = stock_list[:100]
    
    # 替换行业选择为概念选择
    # stock_list = get_stock_industry(stock_list)  # 原行业选择
    stock_list = get_concept_stock_list(context, stock_list)  # 新概念选则
    
    final_list = stock_list[:g.stock_num*2]
    log.info('今日前10:%s' % final_list)
    return final_list


#1-3 整体调整持仓
@profile
def weekly_adjustment(context):
    print("---------------------weekly_adjustment---------------------")
    if g.no_trading_today_signal == False:
        close_no_trading_hold(context)
        #获取应买入列表 
        g.not_buy_again = []
        g.target_list = get_stock_list(context)

        target_list = g.target_list[:g.stock_num*2]
        log.info(str(target_list))

        #调仓卖出
        for stock in g.hold_list:
            if (stock not in target_list) and (stock not in g.yesterday_HL_list):
                log.info("卖出[%s]" % (stock))
                position = context.portfolio.positions[stock]
                close_position(position)
            else:
                pass
                log.info("已持有[%s]" % (stock))
        #调仓买入
        buy_security(context,target_list)
        #记录已买入股票
        for position in list(context.portfolio.positions.values()):
            stock = position.security
            g.not_buy_again.append(stock)


#1-4 调整昨日涨停股票
@profile
def check_limit_up(context):
    now_time = context.current_dt
    if g.yesterday_HL_list != []:
        #对昨日涨停股票观察到尾盘如不涨停则提前卖出，如果涨停即使不在应买入列表仍暂时持有
        for stock in g.yesterday_HL_list:
            if stock in context.portfolio.positions and context.portfolio.positions[stock].closeable_amount > 0:
                current_data = get_price(stock, end_date=now_time, frequency='1m', fields=['close','high_limit'], skip_paused=False, fq='pre', count=1, panel=False, fill_paused=True)
                if current_data.iloc[0,0] <    current_data.iloc[0,1]:
                    log.info("[%s]涨停打开，卖出" % (stock))
                    position = context.portfolio.positions[stock]
                    close_position(position)
                    g.reason_to_sell = 'limitup'
                else:
                    log.info("[%s]涨停，继续持有" % (stock))


#1-5 如果昨天有股票卖出或者买入失败，剩余的金额今天早上买入
@profile
def check_remain_amount(context):
    if g.reason_to_sell == 'limitup': #判断提前售出原因，如果是涨停售出则次日再次交易，如果是止损售出则不交易
        g.hold_list= []
        for position in list(context.portfolio.positions.values()):
            stock = position.security
            g.hold_list.append(stock)
        if len(g.hold_list) < g.stock_num:
            target_list = get_stock_list(context)
            #剔除本周一曾买入的股票，不再买入
            target_list = filter_not_buy_again(target_list)
            target_list = target_list[:min(g.stock_num, len(target_list))]
            log.info('有余额可用'+str(round((context.portfolio.available_cash),2))+'元。'+ str(target_list))
            buy_security(context,target_list)
        g.reason_to_sell = ''

    else:
        # log.info('虽然有余额（'+str(round((context.portfolio.available_cash),2))+'元）可用，但是为止损后余额，下周再交易')
        g.reason_to_sell = ''


#1-6 下午检查交易
@profile
def trade_afternoon(context):
    print("---------------------trade_afternoon---------------------")
    if g.no_trading_today_signal == False:
        check_limit_up(context)
        if g.HV_control == True:
            check_high_volume(context)
        huanshou(context)
        check_remain_amount(context)


#1-7 止盈止损
@profile
def sell_stocks(context):
    print("---------------------sell_stocks---------------------")
    positions_to_sell = list(context.portfolio.positions.keys())
    if g.run_stoploss == True:
        if g.stoploss_strategy == 1:
            for stock in positions_to_sell:
                # 股票盈利大于等于100%则卖出
                if context.portfolio.positions[stock].price >= context.portfolio.positions[stock].avg_cost * 2:
                    order_target_value(stock, 0)
                    log.debug("收益100%止盈,卖出{}".format(stock))
                # 止损
                elif context.portfolio.positions[stock].price < context.portfolio.positions[stock].avg_cost * g.stoploss_limit:
                    order_target_value(stock, 0)
                    log.debug("收益止损,卖出{}".format(stock))
                    g.reason_to_sell = 'stoploss'
        elif g.stoploss_strategy == 2:
            stock_df = get_price(security=get_index_stocks('399101.XSHE'), end_date=context.previous_date, frequency='daily', fields=['close', 'open'], count=1,panel=False)
            #down_ratio = (stock_df['close'] / stock_df['open'] < 1).sum() / len(stock_df)
            #down_ratio = abs((stock_df['close'] / stock_df['open'] - 1).mean())
            down_ratio = (stock_df['close'] / stock_df['open']).mean()
            if down_ratio <= g.stoploss_market:
                g.reason_to_sell = 'stoploss'
                log.debug("大盘惨跌,平均降幅{:.2%}".format(down_ratio))
                for stock in positions_to_sell:
                    order_target_value(stock, 0)
        elif g.stoploss_strategy == 3:
            stock_df = get_price(security=get_index_stocks('399101.XSHE'), end_date=context.previous_date, frequency='daily', fields=['close', 'open'], count=1,panel=False)
            #down_ratio = abs((stock_df['close'] / stock_df['open'] - 1).mean())
            down_ratio = (stock_df['close'] / stock_df['open']).mean().mean()
            if down_ratio <= g.stoploss_market:
                g.reason_to_sell = 'stoploss'
                log.debug("大盘惨跌,平均降幅{:.2%}".format(down_ratio))
                for stock in positions_to_sell:
                    order_target_value(stock, 0)
            else:
                if len(positions_to_sell) == 0:
                    return
                prices = get_price(positions_to_sell, end_date=context.current_dt, frequency='1m', fields=['close'], skip_paused=False, fq='none', count=1, panel=False, fill_paused=True) # 注意使用不复权
                prices.index = prices['code']
                for stock in positions_to_sell:
                    # if context.portfolio.positions[stock].price < context.portfolio.positions[stock].avg_cost * g.stoploss_limit: # context.portfolio.positions[stock].price有问题，不是当前时刻股票的价格
                    if prices.close[stock] < context.portfolio.positions[stock].avg_cost * g.stoploss_limit:
                        order_target_value(stock, 0)
                        log.debug("收益止损,卖出{}".format(stock))
                        g.reason_to_sell = 'stoploss'


# 3-2 调整放量股票
@profile
def check_high_volume(context):
    current_data = get_current_data()
    current_positions = list(context.portfolio.positions.keys())
    for stock in current_positions:
        if current_data[stock].paused == True:
            continue
        if current_data[stock].last_price == current_data[stock].high_limit:
            continue
        if context.portfolio.positions[stock].closeable_amount ==0:
            continue
        curr_volume = get_price(stock, end_date=context.current_dt, frequency='1m', fields=['volume'])
        curr_volume = curr_volume['volume'].values[-1]
        df_volume = get_bars(stock,count=g.HV_duration,unit='1d',fields=['volume'],include_now=False, df=True)
        if curr_volume > g.HV_ratio*df_volume['volume'].values.max():
            log.info("[%s]天量，卖出" % stock)
            position = context.portfolio.positions[stock]
            close_position(position)


#2-7 删除本周一买入的股票
@profile
def filter_not_buy_again(stock_list):
    return [stock for stock in stock_list if stock not in g.not_buy_again]


#换手率计算
@profile
def huanshoulv(context, stock, is_avg=False):
    if is_avg:
        # 计算平均换手率
        start_date = context.current_dt - datetime.timedelta(days=20)
        end_date = context.previous_date
        df_volume = get_price(stock,end_date=end_date, frequency='daily', fields=['volume'],count=20)
        df_cap = get_valuation(stock, end_date=end_date, fields=['circulating_cap'], count=1)
        circulating_cap = df_cap['circulating_cap'].iloc[0] if not df_cap.empty else 0
        if circulating_cap == 0:
            return 0.0
        df_volume['turnover_ratio'] = df_volume['volume'] / (circulating_cap * 10000)
        return df_volume['turnover_ratio'].mean()
    else:
        # 计算实时换手率
        date_now = context.current_dt
        df_vol = get_price(stock, start_date=date_now.date(), end_date=date_now, frequency='1m', fields=['volume'],
                           skip_paused=False, fq='pre', panel=True, fill_paused=False)
        volume = df_vol['volume'].sum()
        date_pre = context.previous_date
        df_circulating_cap = get_valuation(stock, end_date=date_pre, fields=['circulating_cap'], count=1)
        circulating_cap = df_circulating_cap['circulating_cap'].iloc[0]  if not df_circulating_cap.empty else 0
        if circulating_cap == 0:
            return 0.0
        turnover_ratio = volume / (circulating_cap * 10000)
        return turnover_ratio            


# 换手检测
@profile
def huanshou(context):
    ss = []
    current_data = get_current_data()
    shrink, expand = 0.003, 0.1
    current_positions = list(context.portfolio.positions.keys())
    for stock in current_positions:
        if current_data[stock].paused == True:
            continue
        if current_data[stock].last_price >= current_data[stock].high_limit*0.97:
            continue
        if context.portfolio.positions[stock].closeable_amount ==0:
            continue
        rt = huanshoulv(context, stock, False)
        avg = huanshoulv(context, stock, True)
        if avg == 0: continue
        r = rt / avg
        action, icon = '', ''
        if avg < 0.003:
            action, icon = '缩量', '❄️'
        elif rt > expand and r > 2:
            action, icon = '放量', '🔥'
        if action:
            log.info(f"{action} {stock} {get_security_info(stock).display_name} 换手率:{rt:.2%}→均:{avg:.2%} 倍率:{r:.1f}x {icon}")
            position = context.portfolio.positions[stock]
            close_position(position)
            g.reason_to_sell = 'limitup'
    
       
#3-1 交易模块-自定义下单
@profile
def order_target_value_(security, value):
    if value == 0:
        pass
        #log.debug("Selling out %s" % (security))
    else:
        pass
        # log.debug("Order %s to value %f" % (security, value))
    return order_target_value(security, value)


#3-2 交易模块-开仓
@profile
def open_position(security, value):
    order = order_target_value_(security, value)
    if order != None and order.filled > 0:
        return True
    return False


#3-3 交易模块-平仓
@profile
def close_position(position):
    security = position.security
    order = order_target_value_(security, 0)  # 可能会因停牌失败
    if order != None:
        if order.status == OrderStatus.held and order.filled == order.amount:
            return True
    return False


#3-4 买入模块
@profile
def buy_security(context,target_list,cash=0,buy_number=0):
    #调仓买入
    position_count = len(context.portfolio.positions)
    target_num = g.stock_num
    if cash == 0:
        cash = context.portfolio.total_value #cash
    if buy_number == 0:
        buy_number = target_num
    bought_num = 0
    print('---------------------buy_number：%s'%buy_number)
    if target_num > position_count:
        value = cash / (target_num) # - position_count
        for stock in target_list:
            if stock not in context.portfolio.positions or \
                (stock in context.portfolio.positions and context.portfolio.positions[stock].total_amount == 0):
                if bought_num < buy_number:
                    if open_position(stock, value):
                        # log.info("买入[%s]（%s元）" % (stock,value))
                        g.not_buy_again.append(stock) #持仓清单，后续不希望再买入
                        bought_num += 1
                        if len(context.portfolio.positions) == target_num:
                            break


#4-1 判断今天是否为四月
@profile
def today_is_between(context):
    today = context.current_dt.strftime('%m-%d')
    if g.pass_april is True:
        if (('04-01' <= today) and (today <= '04-30')) or (('01-01' <= today) and (today <= '01-30')):
            return True
        else:
           return False
    else:
        return False


#4-2 清仓后次日资金可转
@profile
def close_account(context):
    print("---------------------close_account---------------------")
    if g.no_trading_today_signal == True:
        if len(g.hold_list) != 0 and g.no_trading_hold_signal == False:
            for stock in g.hold_list:
                position = context.portfolio.positions[stock]
                if close_position(position):
                    log.info("卖出[%s]" % (stock))
                else:
                    log.info("卖出[%s]错误！！！！！" % (stock))
            buy_security(context, g.no_trading_buy)
            g.no_trading_hold_signal = True   
            

#4-3 清仓小市值不交易期间股票
@profile
def close_no_trading_hold(context):
    if g.no_trading_hold_signal == True:
        for stock in g.hold_list:
            position = context.portfolio.positions[stock]
            close_position(position)
            log.info("卖出[%s]" % (stock))
        g.no_trading_hold_signal = False


#1-8 动态调仓代码
@profile
def adjust_stock_num(context):
    ma_para = 10  # 设置MA参数
    today = context.previous_date
    start_date = today - datetime.timedelta(days = ma_para*2)
    index_df = get_price('399101.XSHE', start_date=start_date, end_date=today, frequency='daily')
    index_df['ma'] = index_df['close'].rolling(window=ma_para).mean()
    last_row = index_df.iloc[-1]
    diff = last_row['close'] - last_row['ma']
    # 根据差值结果返回数字
    result = 3 if diff >= 500 else \
             3 if 200 <= diff < 500 else \
             4 if -200 <= diff < 200 else \
             5 if -500 <= diff < -200 else \
             6
    return result

@profile
def print_position_info(context):
    print('———————————————————————————————————')
    for position in list(context.portfolio.positions.values()):
        securities=position.security
        cost=position.avg_cost
        price=position.price
        ret=100*(price/cost-1)
        value=position.value
        amount=position.total_amount    
        print('代码:{}'.format(securities))
        print('收益率:{}%'.format(format(ret,'.2f')))
        print('持仓(股):{}'.format(amount))
        print('市值:{}'.format(format(value,'.2f')))
        print('———————————————————————————————————')
    print('余额:{}'.format(format(context.portfolio.available_cash,'.2f')))
    print('———————————————————————————————————————分割线————————————————————————————————————————')


@profile
def get_concept_stock_list(context, stock_list, momentum_days=7, top_k_concepts=10):
    """
    根据概念板块热门度选择股票
    
    参数:
        context: 策略上下文
        stock_list: 初始股票列表
        
    返回:
        基于热门概念选择的股票列表
    """
    start_time = py_time.time()
    # 获取所有概念板块
    concepts_df = get_concepts()
    concepts_df = concepts_df[concepts_df['start_date'] <= context.current_dt]
    
    # 计算每个概念的热门度（以概念动量为例）
    concept_hotness = {}

    momentum_days_end_prices_df = get_price(stock_list, end_date=context.previous_date, count=1, fields=['close'])
    momentum_days_start_prices_df = get_price(stock_list, end_date=context.previous_date - timedelta(days=momentum_days), count=1, fields=['close'])
    stock_momentum = (momentum_days_end_prices_df['close'].values - momentum_days_start_prices_df['close'].values) / momentum_days_start_prices_df['close'].values * 100
    
    for concept_id in concepts_df.index:
        # 获取该概念下的所有股票
        concept_stocks = get_concept_stocks(concept_id, date=context.previous_date)
        
        # 过滤掉不在初始股票列表中的股票
        filtered_stocks = [stock for stock in concept_stocks if stock in stock_list]
        
        if not filtered_stocks:
            continue
            
        # 计算概念板块动量（使用概念内股票的平均涨幅）
        momentum = 0
        for stock in filtered_stocks:
            momentum += stock_momentum[stock_list.index(stock)]

        if len(filtered_stocks) > 0:
            momentum /= len(filtered_stocks)
            concept_hotness[concept_id] = {
                'name': concepts_df.loc[concept_id, 'name'],
                'momentum': momentum,
                'stocks': filtered_stocks
            }
    
    # 按热门度排序概念
    sorted_concepts = sorted(
        concept_hotness.items(), 
        key=lambda x: x[1]['momentum'], 
        reverse=True
    )
    
    # 从热门概念中选择股票
    selected_stocks = []
    selected_concepts = []
    
    for concept_id, concept_info in sorted_concepts:
        if len(selected_concepts) >= top_k_concepts:
            break
        concept_name = concept_info['name']
        if concept_name not in selected_concepts:
            selected_concepts.append(concept_name)
            
            # 从该概念中选择市值最小的股票
            concept_stocks = concept_info['stocks']
            if concept_stocks:
                # 获取这些股票的市值数据
                q = query(
                    valuation.code,
                    valuation.market_cap
                ).filter(valuation.code.in_(concept_stocks)).order_by(valuation.market_cap.asc())
                
                df = get_fundamentals(q, date=context.previous_date)
                if not df.empty:
                    # 选择市值最小的股票
                    selected_stock = df['code'][0]
                    selected_stocks.append(selected_stock)
                    log.info(f"热门概念: {concept_name} (动量: {concept_info['momentum']:.2f}%), 选择股票: {selected_stock}")
    
    return selected_stocks
