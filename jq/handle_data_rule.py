# -*- coding:utf-8 -*- 

from rule import *
from util import *
import pandas as pd

'''==============================选股 stock_list过滤器基类=============================='''
class Handle_data_rule(Rule):
    __name__='Handle_data_rule'
    def before_trading_start(self, context):
        return None
    def handle_data(self,context,data):
        return None
    def after_trading_end(self, context):
        return None

class Handle_data_df(Handle_data_rule):
    __name__='Handle_data_df'

    def before_trading_start(self, context):

        today = context.current_dt.date();

        #清空历史数据
        context.bar_15 = {}
        context.bar_30 = {}
        context.bar_60 = {}

        for stock in context.stock_list:

            #初始化30分钟线数据
            context.bar_30[stock] = attribute_history(stock, 150, unit = '30m', fields = ['close', 'high', 'low'], skip_paused=True, df=True, fq='pre')
            context.bar_60[stock] = attribute_history(stock, 150, unit = '60m', fields = ['close', 'high', 'low'], skip_paused=True, df=True, fq='pre')
            context.bar_15[stock] = attribute_history(stock, 150, unit = '15m', fields = ['close', 'high', 'low'], skip_paused=True, df=True, fq='pre')

        #持仓列表
        for stock in context.portfolio.positions.keys():
            #且不在选股池列表中
            if stock not in context.stock_list:
                #初始化30分钟线数据
                context.bar_30[stock] = attribute_history(stock, 150, unit = '30m', fields = ['close', 'high', 'low'], skip_paused=True, df=True, fq='pre')
                context.bar_60[stock] = attribute_history(stock, 150, unit = '60m', fields = ['close', 'high', 'low'], skip_paused=True, df=True, fq='pre')
                context.bar_15[stock] = attribute_history(stock, 150, unit = '15m', fields = ['close', 'high', 'low'], skip_paused=True, df=True, fq='pre')

        #-------------------------------------------#
        #计算数据
        for stock in context.bar_15.keys():
            context.bar_15[stock]['bottom_alert'] = pd.DataFrame(None, index = context.bar_15[stock].index, columns = ['bottom_alert'])
            context.bar_15[stock]['bottom_buy'] = pd.DataFrame(None, index = context.bar_15[stock].index, columns = ['bottom_buy'])
            #context.bar_15[stock] = macd_alert_calculation(context.bar_15[stock]); 

            '''
            if context.bar_15[stock].iloc[-1]['bottom_alert'] == 1:
                print(get_security_info(stock).display_name, "15分钟钝化", stock)
            if context.bar_15[stock].iloc[-1]['bottom_buy'] == 1:
                print(get_security_info(stock).display_name, "15分钟结构", stock)
            '''

        for stock in context.bar_30.keys():
            context.bar_30[stock]['bottom_alert'] = pd.DataFrame(None, index = context.bar_30[stock].index, columns = ['bottom_alert'])
            context.bar_30[stock]['bottom_buy'] = pd.DataFrame(None, index = context.bar_30[stock].index, columns = ['bottom_buy'])
            #context.bar_30[stock] = macd_alert_calculation(context.bar_30[stock]); 

            '''
            if context.bar_30[stock].iloc[-1]['bottom_alert'] == 1:
                print(get_security_info(stock).display_name, "30分钟钝化", stock)
            if context.bar_30[stock].iloc[-1]['bottom_buy'] == 1:
                print(get_security_info(stock).display_name, "30分钟结构", stock)
            '''

        for stock in context.bar_60.keys():
            context.bar_60[stock]['bottom_alert'] = pd.DataFrame(None, index = context.bar_60[stock].index, columns = ['bottom_alert'])
            context.bar_60[stock]['bottom_buy'] = pd.DataFrame(None, index = context.bar_60[stock].index, columns = ['bottom_buy'])
            #context.bar_60[stock] = macd_alert_calculation(context.bar_60[stock]); 

            '''
            if context.bar_60[stock].iloc[-1]['bottom_alert'] == 1:
                print(get_security_info(stock).display_name, "60分钟钝化", stock)
            if context.bar_60[stock].iloc[-1]['bottom_buy'] == 1:
                print(get_security_info(stock).display_name, "60分钟结构", stock)
            ''' 
        

        #次新指数
        context.index_df = attribute_history('399678.XSHE', 150, unit = '1d', fields = ['close', 'high', 'low'], skip_paused=True, df=True, fq='pre')
        context.index_df['bottom_alert'] = pd.DataFrame(None, index = context.index_df.index, columns = ['bottom_alert'])
        context.index_df['bottom_buy'] = pd.DataFrame(None, index = context.index_df.index, columns = ['bottom_buy'])        
        context.index_df = macd_alert_calculation(context.index_df)

        '''
        context.index_df_30 = pd.DataFrame(history_bars('399678.XSHE', 150, frequency = '30m', fields = ['close', 'high', 'low'], include_now = True), index = None)
        context.index_df_30['bottom_alert'] = pd.DataFrame(None, index = context.index_df_30.index, columns = ['bottom_alert'])
        context.index_df_30['bottom_buy'] = pd.DataFrame(None, index = context.index_df_30.index, columns = ['bottom_buy'])  
        context.index_df_30 = macd_alert_calculation(context.index_df_30)

        if context.index_df_30.iloc[-1]['bottom_buy'] == 1:
            print("399678指数 30分钟底部结构")
        '''
        
        #自选股评分
        stock_score(context)

        return None

    
    def handle_data(self,context,data):

        #分钟线数据制作
        for stock in context.stock_list:
            self.handle_minute_data(context, data, stock)
            
        for stock in context.portfolio.positions.keys():
            if stock not in context.stock_list:
                self.handle_minute_data(context,data,stock)

        #选股评分
        #if context.timedelt % 60 == 0:
            #stock_score(context, data)
    
        #指数分时线结构
        '''
        if context.timedelt % 30 == 0:

            context.index_df_30 = attribute_history('399678.XSHE', 150, unit = '30m', fields = ['close', 'high', 'low'], skip_paused=True, df=True, fq='pre')
            context.index_df_30['bottom_alert'] = pd.DataFrame(None, index = context.index_df_30.index, columns = ['bottom_alert'])
            context.index_df_30['bottom_buy'] = pd.DataFrame(None, index = context.index_df_30.index, columns = ['bottom_buy'])  
            context.index_df_30 = macd_alert_calculation(context.index_df_30)

            if context.index_df_30.iloc[-1]['bottom_buy'] == 1:
                print("399678指数 30分钟底部结构")
        '''

        #指数日线快照
        if context.timedelt % 30 == 0 and context.timedelt != 0 or context.timedelt == 235:

            temp_data = pd.DataFrame(
                {"close":history(1, unit = '1m', field = ('close'), security_list = ['399678.XSHE'], df = False, skip_paused = True)['399678.XSHE'][0],
                "high":history(30, unit = '1m', field = ('high'), security_list = ['399678.XSHE'], df = False, skip_paused = True)['399678.XSHE'].max(),
                "low":history(30, unit = '1m', field = ('low'), security_list = ['399678.XSHE'], df = False, skip_paused = True)['399678.XSHE'].min(),
                "bottom_alert":'',
                "bottom_buy":'',
                "diff":'',
                "dea":'',
                "macd":''
                }, index = ["0"])

            #context.index_df = context.index_df.append(temp_data, ignore_index = True)
            #context.index_df = macd_alert_calculation(context.index_df)


    def handle_minute_data(self, context, data, stock):

        if context.timedelt % 15 == 0 and context.timedelt != 0 or context.timedelt == 235:

            temp_data = pd.DataFrame(
                {"close":history(1, unit = '1m', field = ('close'), security_list = [stock], df = False, skip_paused = True)[stock][0],
                "high":history(15, unit = '1m', field = ('high'), security_list = [stock], df = False, skip_paused = True)[stock].max(),
                "low":history(15, unit = '1m', field = ('low'), security_list = [stock], df = False, skip_paused = True)[stock].min(),
                "bottom_alert":'',
                "bottom_buy":'',
                "diff":'',
                "dea":'',
                "macd":''
                }, index = ["0"])
            
            context.bar_15[stock] = context.bar_15[stock].append(temp_data, ignore_index = True)
            #context.bar_15[stock] = macd_alert_calculation(context.bar_15[stock])
            
            '''
            if context.bar_15[stock].iloc[-1]['bottom_alert'] == 1:
                print(get_security_info(stock).display_name, "15分钟钝化", stock)
            if context.bar_15[stock].iloc[-1]['bottom_buy'] == 1:
                print(get_security_info(stock).display_name, "15分钟结构", stock)
            '''
            
        if context.timedelt % 30 == 0 and context.timedelt != 0 or context.timedelt == 235:
            
            temp_data = pd.DataFrame(
                {"close":history(1, unit = '1m', field = ('close'), security_list = [stock], df = False, skip_paused = True)[stock][0],
                "high":history(30, unit = '1m', field = ('high'), security_list = [stock], df = False, skip_paused = True)[stock].max(),
                "low":history(30, unit = '1m', field = ('low'), security_list = [stock], df = False, skip_paused = True)[stock].min(),
                "bottom_alert":'',
                "bottom_buy":'',
                "diff":'',
                "dea":'',
                "macd":''
                }, index = ["0"])

            context.bar_30[stock] = context.bar_30[stock].append(temp_data, ignore_index = True)
            #context.bar_30[stock] = macd_alert_calculation(context.bar_30[stock])

            '''
            if context.bar_30[stock].iloc[-1]['bottom_alert'] == 1:
                print(get_security_info(stock).display_name, "30分钟钝化", stock)
            if context.bar_30[stock].iloc[-1]['bottom_buy'] == 1:
                print(get_security_info(stock).display_name, "30分钟结构", stock)
            '''

        if context.timedelt % 60 == 0 and context.timedelt != 0 or context.timedelt == 235:
            
            temp_data = pd.DataFrame(
                {"close":history(1, unit = '1m', field = ('close'), security_list = [stock], df = False, skip_paused = True)[stock][0],
                "high":history(60, unit = '1m', field = ('high'), security_list = [stock], df = False, skip_paused = True)[stock].max(),
                "low":history(60, unit = '1m', field = ('low'), security_list = [stock], df = False, skip_paused = True)[stock].min(),
                "bottom_alert":'',
                "bottom_buy":'',
                "diff":'',
                "dea":'',
                "macd":''
                }, index = ["0"])

            context.bar_60[stock] = context.bar_60[stock].append(temp_data, ignore_index = True)

            '''
            if context.bar_60[stock].iloc[-1]['bottom_alert'] == 1:
                print(get_security_info(stock).display_name, "60分钟钝化", stock)
            if context.bar_60[stock].iloc[-1]['bottom_buy'] == 1:
                print(get_security_info(stock).display_name, "60分钟结构", stock)
            '''

    def after_trading_end(self, context):

        stock_score(context)

        return None


    def __str__(self):
        return '分钟数据处理'