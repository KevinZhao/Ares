# -*- coding:utf-8 -*- 

from kuanke.user_space_api import *
import talib
import pandas as pd 
import numpy as np

# 通过类的类型得到已创建的对象实例
def get_obj_by_class_type(class_type):
    for rule in g.all_rules:
        if rule.__class__ == class_type:
            return rule

''' ==============================持仓操作函数，共用================================'''
# 开仓，买入指定价值的证券
# 报单成功并成交（包括全部成交或部分成交，此时成交量大于0），返回True
# 报单失败或者报单成功但被取消（此时成交量等于0），返回False
# 报单成功，触发所有规则的when_buy_stock函数

def open_position(security, value):
    order = order_target_value_(security, value)

    if order != None and order.filled > 0:
        for rule in g.all_rules:
            rule.when_buy_stock(security,order)
        return True
    return False


def open_position_by_percent(context, security, percent):
    order = order_target_value_by_percent(context, security, percent)

    if order != None and order.filled > 0:
        for rule in g.all_rules:
            rule.when_buy_stock(security,order)
        return True
    return False

# 平仓，卖出指定持仓
# 平仓成功并全部成交，返回True
# 报单失败或者报单成功但被取消（此时成交量等于0），或者报单非全部成交，返回False
# 报单成功，触发所有规则的when_sell_stock函数
def close_position(position, is_normal = True):
    security = position.security
    order = order_target_value_(security, 0) # 可能会因停牌失败
    if order != None:
        #todo:
        #if order.filled > 0:
        for rule in g.all_rules:
            rule.when_sell_stock(position,order,is_normal)
        return True
    return False

# 平仓，卖出指定持仓
# 平仓成功并全部成交，返回True
# 报单失败或者报单成功但被取消（此时成交量等于0），或者报单非全部成交，返回False
# 报单成功，触发所有规则的when_sell_stock函数
'''
def close_position_2(position, percentage, is_normal = True):
    
    security = position.order_book_id
    #percentage = context.
    order = order_target_percent(security, percentage) # 可能会因停牌失败
    if order != None:
        #todo:
        #if order.filled > 0:
        for rule in g.all_rules:
            rule.when_sell_stock(position,order,is_normal)
        return True
    return False
'''


# 清空卖出所有持仓
# 清仓时，调用所有规则的 when_clear_position
def clear_position(context):
    if context.portfolio.positions:
        log.info("==> 清仓，卖出所有股票")
        for stock in context.portfolio.positions.keys():
            position = context.portfolio.positions[stock]
            close_position(position,False)
    for rule in g.all_rules:
        rule.when_clear_position(context)        

# 自定义下单
# 根据Joinquant文档，当前报单函数都是阻塞执行，报单函数（如order_target_value）返回即表示报单完成
# 报单成功返回报单（不代表一定会成交），否则返回None
def order_target_value_(security, value):
    if value == 0:
        log.debug("Selling out %s" % (security))
    else:
        log.debug("Order %s to value %f" % (security, value))
        
    # 如果股票停牌，创建报单会失败，order_target_value 返回None
    # 如果股票涨跌停，创建报单会成功，order_target_value 返回Order，但是报单会取消
    # 部成部撤的报单，聚宽状态是已撤，此时成交量>0，可通过成交量判断是否有成交
    return order_target_value(security, value)

def order_target_value_by_percent(context, security, percent):
    if percent == 0:
        log.debug("Selling out %s" % (security))
    else:
        log.debug("Order %s to percent %f" % (security, percent))

    value = context.portfolio.total_value * percent
        
    # 如果股票停牌，创建报单会失败，order_target_value 返回None
    # 如果股票涨跌停，创建报单会成功，order_target_value 返回Order，但是报单会取消
    # 部成部撤的报单，聚宽状态是已撤，此时成交量>0，可通过成交量判断是否有成交
    return order_target_value(security, value)

        
'''~~~~~~~~~~~~~~~~~~~~~~~~~~~基础函数~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'''

#计算MACD值
def calculate_macd(df):

    diff, dea, macd = talib.MACD(df['close'].values, 12, 26, 9)

    df['diff'] = pd.DataFrame(diff, index = df.index, columns = ['diff'])
    df['dea'] = pd.DataFrame(dea, index = df.index, columns = ['dea'])
    df['macd'] = pd.DataFrame(macd*2, index = df.index, columns = ['macd'])

    return df

def macd_alert_calculation(macd_df):
    
    diff, dea, macd = talib.MACD(macd_df['close'].values, 12, 26, 9)
                   
    #计算MACD值                     
    macd_df['diff'] = pd.DataFrame(diff, index = macd_df.index, columns = ['diff'])
    macd_df['dea'] = pd.DataFrame(dea, index = macd_df.index, columns = ['dea'])
    macd_df['macd'] = pd.DataFrame(macd*2, index = macd_df.index, columns = ['macd'])
    
    #1. 计算波峰跨度
    n1 = 0;
    n2 = 0;
    n3 = 0;

    df_count = len(macd_df.index)
    
    #2. 寻找前一个区间, #当前macd大于零
    if macd_df.iloc[-1]['macd'] > 0:
        for i in range(1, 150):
            if (-1 - i) > -df_count:
                if macd_df.iloc[-1 - i]['macd'] < 0:
                    n1 = i;
                    break;
        
        for i in range(1, 150):
            if (-1 - n1 - i) > -df_count:
                if macd_df.iloc[-1 - n1 -i]['macd'] > 0:
                    n2 = i;
                    break;
      
        for i in range(1, 150):
            if (-1 -n1 -n2 - i) > -df_count:
                if macd_df.iloc[-1 - n1 - n2 -i]['macd'] < 0:
                    n3 = i;
                    break;          
    else:
        for i in range(1, 150):
            if (-1 - i) > -df_count:
                if macd_df.iloc[-1 - i]['macd'] > 0:
                    n1 = i;
                    break;
        
        for i in range(1, 150):
            if (-1 - n1 - i) > -df_count:
                if macd_df.iloc[-1 - n1 -i]['macd'] < 0:
                    n2 = i;
                    break;
      
        for i in range(1, 150):
            if (-1 - n1 - n2 - i) > -df_count:
                if macd_df.iloc[-1 - n1 - n2 -i]['macd'] > 0:
                    n3 = i;
                    break; 

    #本周期和前一个周期的峰值计算
    current_price = macd_df.iloc[-1]['close']
    current_diff = macd_df.iloc[-1]['diff']
    
    current_max_price = macd_df.tail(n1)['close'].max()
    current_min_price = macd_df.tail(n1)['close'].min()
    
    last_max_price = macd_df.iloc[- n1 - n2 - n3:- n1 -n2]['close'].max()
    last_max_diff = macd_df.iloc[- n1 - n2 - n3: - n1 - n2]['diff'].max()
    last_min_price = macd_df.iloc[- n1 - n2 - n3:- n1 -n2]['close'].min()
    last_min_diff = macd_df.iloc[- n1 - n2 - n3: - n1 - n2]['diff'].min()

    #3. 底部钝化判断：macd负值，diff向下, 当前价格为区间内最低价格，有两棵绿脚线
    if (macd_df.iloc[-1]['macd'] < 0) and (macd_df.iloc[-1]['diff'] < macd_df.iloc[-2]['diff']) and (current_price == current_min_price):

        # 价格创新低，diff未创新低
        if (current_price < last_min_price) and (current_diff > last_min_diff):
            macd_df['bottom_alert'][df_count - 1] = 1

    # 钝化消失判断
    # 钝化状态中
    for i in range(1, df_count):
        if macd_df.iloc[-i]['bottom_alert'] == 1:

            # 当前diff 小于 前峰值的最小diff
            if current_diff < last_min_diff:
                macd_df['bottom_alert'] = pd.DataFrame(None, index = macd_df.index, columns = ['bottom_alert'])
            break
        
    # 结构形成判断
    # 钝化状态中
    for i in range(1, df_count):
        if macd_df.iloc[-i]['bottom_alert'] == 1:
            
            # diff向上拐点
            if (current_diff > macd_df.iloc[-2]['diff']): 
                macd_df['bottom_buy'][df_count - 1] = 1
                #print(macd_df)
                break

    return macd_df

            #signals[stock].bottom_buy = True;
            #signals[stock].bottom_buy_diff = current_diff;
            #signals[stock].bottom_alert = False;
            #print(minute_object.frequency, "分钟 底部结构形成", current_diff, current_price)
    
    # 底部结构消失 todo 结构消失的条件需要优化
    #if signals[stock].bottom_buy:
        
        #current price > 形成结构点的price
        #if current_diff < signals[stock].bottom_buy_diff:
        #    signals[stock].bottom_buy = False
            #signals[stock].bottom_period = 0;
            #print(minute_object.frequency, "分钟 底部结构消失", current_diff, current_price)
#------------------------------------------------------------------------------------------------       
'''         
    #3. 顶部钝化判断：macd正值，diff向上
    if macd_df.iloc[-1]['macd'] > 0 and (macd_df.iloc[-1]['diff'] > macd_df.iloc[-2]['diff']) and (current_price == current_max_price):

        # 价格创新高，diff未创新高
        if (current_price > last_max_price) and (current_diff < last_max_diff):
            
            if signals[stock].top_alert != True:
                signals[stock].top_alert = True;
                signals[stock].bottom_buy = False;
                #print(minute_object.frequency, "分钟 顶部钝化形成", current_diff, current_price)
            
    # 钝化消失判断
    if signals[stock].top_alert:
        if current_diff > last_max_diff:
            signals[stock].top_alert = False;
            #print(minute_object.frequency, "分钟 顶部钝化消失", current_diff, current_price);
        
    # 形成结构        
    if signals[stock].top_alert:
    
        if (current_diff < macd_df.iloc[-2]['diff']): #and (n1*2 > n3):
            signals[stock].top_sell = True;
            signals[stock].top_sell_diff = current_diff;
            signals[stock].top_alert = False;
            #print(minute_object.frequency, "分钟 顶部结构形成", current_diff, current_price)
            
            
    #结构消失
    if signals[stock].top_sell:
        
        #current price > 形成结构点的price
        if current_diff > signals[stock].top_sell_diff:
            signals[stock].top_sell = False
            #print(minute_object.frequency, "分钟 顶部结构消失", current_diff, current_price)

class Signal:
    def __init__(self):
        self.top_alert       = False
        self.top_sell        = False
        self.top_sell_diff   = 0
    
        self.bottom_alert    = False
        self.bottom_buy      = False
        self.bottom_buy_diff = 0
        self.bottom_period   = 0
'''

def calculate_macd_index(df):

    diff, dea, macd = talib.MACD(df['close'].values, 12, 26, 9)

    df['diff'] = pd.DataFrame(diff, index = df.index, columns = ['diff'])
    df['dea'] = pd.DataFrame(dea, index = df.index, columns = ['dea'])
    df['macd'] = pd.DataFrame(macd*2, index = df.index, columns = ['macd'])

    return df

def stock_score(context, data = None):

    context.result_df = pd.DataFrame(columns = {'stock', 'score', 'instruments', 'circulation_a', 'days_from_listed', '5day_avg'})

    for stock in context.stock_list:

        score_df = context.bar_60[stock].tail(10)
        score_df = score_df.loc[ : , ['close']]

        score_list = []
            
        for i in range(0, 10):
            h = context.bar_60[stock].tail(130+i)
            h = h.iloc[0:130]

            low_price_130 = h.low.min()
            high_price_130 = h.high.max()
                
            avg_15_df = h.tail(15)
            avg_15 = avg_15_df.close.sum()/15
                
            cur_price = h.iloc[-1]['close']

            score = ((cur_price-low_price_130) + (cur_price-high_price_130) + (cur_price-avg_15))
            score_list.append(score)

        score_list.reverse()
        score_df['score'] = pd.DataFrame(score_list, index = score_df.index, columns = ['score'])

        fiveday_avg = calc_avg(stock, 5, '1d').tolist()
        fiveday_avg.reverse();

        q = query(
            valuation
                    ).filter(
            valuation.code == stock)

        temp_data = pd.DataFrame(
            {"score":score_df.iloc[-1]['score'],
            "stock":stock,
            "instruments":get_security_info(stock).display_name,
            "circulation_a":get_fundamentals(q, context.current_dt.date()).circulating_cap[0]* context.bar_60[stock].iloc[-1]['close']/10000,
            "days_from_listed":get_security_info(stock).start_date,
            "5day_avg": fiveday_avg[0]
            }, index = ["0"])

        context.result_df = context.result_df.append(temp_data, ignore_index= True)
        context.score_df[stock] = score_df

    context.result_df = context.result_df.sort(columns='circulation_a', ascending=True)
    context.stock_list = list(context.result_df.stock)

    if context.debug == False:
        print(context.result_df)

    return None

# 获取股票n日以来涨幅，根据当前价计算
# n 默认20日
def get_growth_rate(security, n=20):

    lc = get_close_price(security, n)
    c = get_close_price(security, 1)

    if not isnan(lc) and not isnan(c) and lc != 0:
        return (c - lc) / lc
    else:
        log.error("数据非法, security: %s, %d日收盘价: %f, 当前价: %f" %(security, n, lc, c))
        return 0

# 获取前n个单位时间当时的收盘价
def get_close_price(security, n, unit='1d'):

    return attribute_history(security, n, unit, fields = 'close', skip_paused=True, df=False, fq='pre')['close'][0]

def isnan(value):
    if value == None:
        return True
    else:
        return False

def findATR(context, bar, stock):

    history_df = bar[stock].tail(context.ATRperiod+2)
        
    close = history_df['close'][0:context.ATRperiod]
    high = history_df['high'][1:context.ATRperiod+1]
    low = history_df['low'][1:context.ATRperiod+1]
        
    art1=high.values-low.values
    art2=abs(close.values - high.values)
    art3=abs(close.values - low.values)
    art123=np.matrix([art1, art2, art3])
        
    rawatr=np.array(art123.max(0)).flatten()
    ATR=rawatr.sum()/len(rawatr)

    return ATR

def createdic(context, data, stock):
    if stock not in context.maxvalue.columns:
        temp = pd.DataFrame({str(stock):[0]})    
        context.maxvalue = pd.concat([context.maxvalue, temp], axis=1, join='inner')

def calc_avg(stock, period, frequency):

    #print(stock)
    #print(attribute_history(stock, period*2, unit = frequency, fields = 'close', skip_paused=True, df=False, fq='pre')['close'])

    result = talib.MA(attribute_history(stock, period*2, unit = frequency, fields = 'close', skip_paused=True, df=False, fq='pre')['close'], timeperiod = period)
    
    return result
