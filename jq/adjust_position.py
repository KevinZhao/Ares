# -*- coding:utf-8 -*- 

from kuanke.user_space_api import *
from rule import *
from util import *

'''==============================调仓的操作基类=============================='''
class Adjust_position(Rule):
    def adjust(self,context,data,buy_stocks):
        pass

''' ---------个股止损 by 自最高值回落一定比例比例进行止损-------------------------'''
class Stop_loss_stocks_by_percentage(Rule):
    __name__='Stop_loss_stocks_by_percentage'

    def __init__(self,params):
        self.percent = params.get('percentage', 0.08)

    def update_params(self,context,params):
        self.percent = params.get('percentage', self.percentage)
        
    # 个股止损
    def handle_data(self,context, data):

        #持仓股票循环
        for stock in context.portfolio.positions.keys():
            
            #持有数量超过0
            if context.portfolio.positions[stock].closeable_amount > 0:

                #当前价格
                cur_price = data[stock].close
                
                #历史最高价格
                if stock in context.maxvalue.keys():
                    stockdic = context.maxvalue[stock]
                    highest = stockdic[0]
                else:
                    highest = data[stock].high

                if data[stock].high > highest:

                    del context.maxvalue[stock]        
                    temp = pd.DataFrame({str(stock):[max(highest, data[stock].high)]})
                    context.maxvalue = pd.concat([context.maxvalue, temp], axis=1, join='inner') # 更新其盘中最高价值和先阶段比例。

                    #更新历史最高价格
                    stockdic = context.maxvalue[stock]
                    highest = stockdic[0]

                if cur_price < highest * (1 - self.percent):
                    position = context.portfolio.positions[stock]
                    close_position(position, False)
                    context.black_list.append(stock)

                    print('[比例止损卖出]', get_security_info(stock).display_name, context.portfolio.positions[stock].avg_cost, highest, data[stock].close)
            else:
                if context.portfolio.positions[stock].total_amount == 0:
                    #del context.maxvalue[stock]
                    context.ATRList.remove(stock)


    def when_sell_stock(self,position,order,is_normal):
        #if position.security in self.last_high:
        #    self.last_high.pop(position.security)
        pass
    
    def when_buy_stock(self,stock,order):
        #if order.status == OrderStatus.held and order.filled == order.amount:
            # 全部成交则删除相关证券的最高价缓存
        #    self.last_high[stock] = get_close_price(stock, 1, '1m')
        pass
           
    def __str__(self):
        return '个股止损器:[按比例止损: %d ]' %self.percent

''' ----------------------个股止损 by ATR 60-------------------------------------'''
class Stop_loss_stocks_by_ATR(Rule):
    __name__='Stop_loss_stocks_by_ATR'

    def __init__(self,params):
        pass
    def update_params(self,context,params):
        pass
    # 个股止损
    def handle_data(self, context, data):

        for stock in context.ATRList:

            if stock in context.portfolio.positions.keys() and context.portfolio.positions[stock].closeable_amount > 0:

                print('context.maxvalue', context.maxvalue)

                if stock in context.maxvalue.keys():
                    stockdic = context.maxvalue[stock]
                    highest = stockdic[0]
                else:
                    highest = data[stock].high
                    temp = pd.DataFrame({str(stock):[highest]})
                    print(temp)
                    context.maxvalue= pd.concat([context.maxvalue, temp], axis=1, join='inner') # 更新其盘中最高价值和先阶段比例。

                print('context.maxvalue', context.maxvalue)


                #当前涨幅判断
                #raisePercentage = (highest - context.portfolio.positions[stock].avg_cost) /context.portfolio.positions[stock].avg_cost

                '''
                if  raisePercentage > 0.18:

                    bar = context.bar_60
                    minute = '60分钟'

                if  (raisePercentage > 0.12) and (raisePercentage <= 0.18):

                    bar = context.bar_30
                    minute = '30分钟'

                if  (raisePercentage >= 0.06) and (raisePercentage <= 0.12):

                    bar = context.bar_15
                    minute = '15分钟'

                if (raisePercentage < 0.06):
                    return

                ATR = findATR(context, bar, stock)
                '''

                #high = bar[stock].iloc[-1]['high']
                #current = bar[stock].iloc[-1]['close']

                high = data[stock].high
                current = data[stock].close

                del context.maxvalue[stock]        
                temp = pd.DataFrame({str(stock):[max(highest,high)]})

                context.maxvalue= pd.concat([context.maxvalue, temp], axis=1, join='inner') # 更新其盘中最高价值和先阶段比例。
                print('context.maxvalue', context.maxvalue)

                stockdic = context.maxvalue[stock]
                highest = stockdic[0]
        
                #if data[stock].close < highest - 3*ATR:
                if data[stock].close < highest * 0.95:
                
                    print('[ATR止损卖出]', get_security_info(stock).display_name, 
                        context.portfolio.positions[stock].avg_cost, highest, data[stock].close)
                    position = context.portfolio.positions[stock]
                    close_position(position)
                    context.black_list.append(stock)
            else:
                if context.portfolio.positions[stock].total_amount == 0:
                    #del context.maxvalue[stock]
                    context.ATRList.remove(stock)

        
    def when_sell_stock(self,position,order,is_normal):
        #if position.security in self.last_high:
        #    self.last_high.pop(position.security)
        pass
    
    def when_buy_stock(self,stock,order):
        #if order.status == OrderStatus.held and order.filled == order.amount:
            # 全部成交则删除相关证券的最高价缓存
        #    self.last_high[stock] = get_close_price(stock, 1, '1m')
        pass

    def after_trading_end(self,context):
        #self.pct_change = {}
        pass
           
    def __str__(self):
        return '个股止损器:ATR止损'   

'''---------------卖出股票规则------------------------'''
'''---------------个股涨幅超过5%，进入ATR--------------'''  
class Sell_stocks(Adjust_position):
    __name__='Sell_stocks'
    def adjust(self,context,data,buy_stocks):

        print('entering sell stocks')

        if len(context.stock_list) == 0:
            return
        
        for stock in context.portfolio.positions.keys():
            print('stock', stock)
            
            if context.portfolio.positions[stock].closeable_amount == 0:
                continue;
            print(context.stock_list)

                #止损
            if stock not in context.stock_list:
                position = context.portfolio.positions[stock]
                close_position(position)

    def __str__(self):
        return '股票调仓卖出规则：卖出不在buy_stocks的股票'
      
    
'''---------------买入股票规则--------------'''  
class Buy_stocks_position(Adjust_position):
    __name__='Buy_stocks'
    def __init__(self,params):
        self.buy_count = params.get('buy_count', 5)
    def update_params(self,context,params):
        self.buy_count = params.get('buy_count',self.buy_count)
    def adjust(self,context,data,buy_stocks):

        print('entering Buy_stocks_position')

        #计算实际仓位
        actual_position = context.portfolio.positions_value / context.portfolio.total_value

        #避免小额下单
        if context.portfolio.cash < 10000:
            return

        print('holding count', len(context.portfolio.positions.keys()))

        for stock in context.stock_list:

            print('stock', stock)

            #股票在黑名单中
            if stock in context.black_list:
                continue;

            print(actual_position, context.position)

            #如果已经满仓
            #if actual_position > context.position * 0.99:
            #    continue;

            print(len(context.portfolio.positions.keys()), self.buy_count)

            if len(context.portfolio.positions.keys()) >= self.buy_count:
                continue;

            print(stock, context.portfolio.positions.keys())
            #如果股票在stock_list中，且当前没有持仓
            if stock not in context.portfolio.positions.keys():

                open_position_by_percent(context, stock, 1.0/self.buy_count)
                context.ATRList.append(stock)



        pass
    def __str__(self):
        return '股票调仓买入规则：现金平分式买入股票达目标股票数'
        
'''---------------买入股票规则 按底部结构买入--------------'''  
class Buy_stocks_low(Adjust_position):
    __name__='Buy_stocks_low'
    def __init__(self,params):
        self.buy_count = params.get('buy_count', 4)
    def update_params(self,context,params):
        self.buy_count = params.get('buy_count',self.buy_count)
    def adjust(self,context,data,buy_stocks):
        # 买入股票
        # 始终保持持仓数目为g.buy_stock_count
        # 根据股票数量分仓
        # 此处只根据可用金额平均分配购买，不能保证每个仓位平均分配

        #开盘和尾盘不进行交易
        if context.timedelt < 15 or context.timedelt > 237:
            return

        #30分钟线进行交易
        if (context.timedelt % 5 >= 3) or (context.timedelt % 5 == 0): #and (context.timedelt % 60 <= 5):
            return

        for stock in buy_stocks:

            if stock in context.black_list:
                return

            #避免小额下单
            if context.portfolio.cash < 20000:
                return

            macd_df_60 = context.bar_60[stock]
            macd_df_30 = context.bar_30[stock]
            macd_df_15 = context.bar_15[stock]

            #if (context.portfolio.market_value / context.portfolio.total_value) > context.position:
            #    return
                
            #构成买入条件
            if macd_df_60.iloc[-1]['bottom_buy'] == 1:

                createdic(context, data, stock)
                    
                if context.portfolio.positions[stock].value/context.portfolio.total_value * 1.1 < context.position/self.buy_count:
                    open_position_by_percent(context, stock, context.position/self.buy_count)

                    if stock not in context.stock_60:
                        context.stock_60.append(stock)

                    print('[60分钟 底部结构买入]', get_security_info(stock).display_name, context.position/self.buy_count)

            if macd_df_30.iloc[-1]['bottom_buy'] == 1:

                createdic(context, data, stock)
                    
                if context.portfolio.positions[stock].value/context.portfolio.total_value * 1.1 < (context.position/self.buy_count):
                    open_position_by_percent(context, stock, context.position/self.buy_count) 

                    if stock not in context.stock_30:
                        context.stock_30.append(stock)

                    print('[30分钟 底部结构买入]', get_security_info(stock).display_name, context.position/self.buy_count)

            if macd_df_15.iloc[-1]['bottom_buy'] == 1 and context.index_df.iloc[-1]['macd'] > 0:

                createdic(context, data, stock)
                    
                if context.portfolio.positions[stock].value/context.portfolio.total_value * 1.1 < (context.position/self.buy_count):
                    open_position_by_percent(context, stock, context.position/self.buy_count)

                    if stock not in context.stock_15:
                        context.stock_30.append(stock)

                    print('[15分钟 底部结构买入]', get_security_info(stock).display_name, context.position/self.buy_count)

            #else:
                #self.open_position_by_percent(stock, 1/buy_count)

        pass
    def __str__(self):
        return '股票调仓买入规则：现金平分式买入股票达目标股票数'


def generate_portion(num):
    total_portion = num * (num+1) / 2
    start = num
    while num != 0:
        yield float(num) / float(total_portion)
        num -= 1

