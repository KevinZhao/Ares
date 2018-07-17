# -*- coding:utf-8 -*- 

from kuanke.user_space_api import *
from rule import *

'''==============================选股 stock_list过滤器基类=============================='''
class Filter_stock_list(Rule):
    __name__='Filter_stock_list'
    def before_trading_start(self, context):
        return None
    def filter(self,context,data,stock_list):
        return None

class Filter_gem(Filter_stock_list):
    __name__='Filter_gem'
    def before_trading_start(self, context):

        result_list = []

        for stock in context.stock_list:
            if stock[0:3] != '300':
                result_list.append(stock)
                pass

        context.stock_list = result_list

        return None
    def filter(self,context,data,stock_list):
        return [stock for stock in stock_list if stock[0:3] != '300']
    def __str__(self):
        return '过滤创业板股票'

        
class Filter_paused_stock(Filter_stock_list):
    __name__='Filter_paused_stock'
    def before_trading_start(self, context):

        return [stock for stock in context.stock_list
            if not get_current_data()[stock].paused
            ]
    def filter(self,context,data,stock_list):
        return [stock for stock in context.stock_list
            if not get_current_data()[stock].paused
            ]

    def __str__(self):
        return '过滤停牌股票'

class Filter_limitup(Filter_stock_list):
    __name__='Filter_limitup'

    def before_trading_start(self, context):

        #盘前过滤前日一字涨停
        result_list = []
 
        for stock in context.stock_list:
            h = attribute_history(stock, 1, unit = '1d', fields = 'high', skip_paused=True, df=False, fq='pre')
            l = attribute_history(stock, 1, unit = '1d', fields = 'low', skip_paused=True, df=False, fq='pre')
            
            if h['high'][0] != l['low'][0]:
                result_list.append(stock)

        context.stock_list = result_list

        return context.stock_list

    def filter(self,context,data,stock_list):
        threshold = 1.00
        return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
            or data[stock].close < data[stock].high_limit * threshold]

    def __str__(self):
        return '过滤涨停股票'

class Filter_old_stock(Filter_stock_list):
    __name__='Filter_old_stock'
    def __init__(self,params):
        self.day_count_min = params.get('day_count_min', 5)
        self.day_count_max = params.get('day_count_max', 80)

    def before_trading_start(self, context):
        context.stock_list = [stock for stock in context.stock_list 
            if (context.current_dt.date() - get_security_info(stock).start_date).days <= self.day_count_max 
                and (context.current_dt.date()- get_security_info(stock).start_date).days >= self.day_count_min]

        print(len(context.stock_list))

        while len(context.stock_list) < 8:

            self.day_count_max = self.day_count_max + 10;

            context.stock_list = [stock for stock in context.stock_list 
                if (context.current_dt.date() - get_security_info(stock).start_date).days <= self.day_count_max
                    and (context.current_dt.date()- get_security_info(stock).start_date).days >= self.day_count_min]

            if self.day_count_max >= 200:
                break;

        return context.stock_list

    def update_params(self,context,params):
        self.day_count_min = params.get('day_count_min', self.day_count_min)
        self.day_count_max = params.get('day_count_max', self.day_count_max)
    def filter(self,context,data,stock_list):
        return [stock for stock in stock_list 
            if (context.current_dt.date() - get_security_info(stock).start_date).days <= self.day_count_max and (context.current_dt.date() - get_security_info(stock).start_date).days >= self.day_count_min]
    def __str__(self):
        return '过滤上市时间超过 %d 天的股票' %(self.day_count_max)

class Filter_just_open_limit(Filter_stock_list):
    __name__='过滤新开板股票'
    def before_trading_start(self, context):
        for stock in context.stock_list:
            #两种可能，前一天如果是涨停则为开板，前一天不是涨停，就是跌入股票池
            history = history_bars(stock, 2, '1d', 'close')
            if len(history) == 2 and history[0]*1.099 > history[1]:
                result_list.append(stock)

        return context.stock_list
    def filter(self,context,data,stock_list):

        result_list = []

        if context.stock_list == None:
            context.stock_list = stock_list
            result_list = stock_list
        else:
            for stock in stock_list:
                if stock not in context.stock_list:
                    #两种可能，前一天如果是涨停则为开板，前一天不是涨停，就是跌入股票池
                    history = history_bars(stock, 2, '1d', 'close')
                    if len(history) == 2 and history[0]*1.099 > history[1]:
                        result_list.append(stock)
                else:
                    result_list.append(stock)

        return result_list
    def __str__(self):
        return '过滤新开板股票'

class Filter_st(Filter_stock_list):
    __name__='Filter_st'
    def filter(self,context,data,stock_list):
        current_data = get_current_data()
        return [stock for stock in stock_list
            if not is_st_stock(stock)
            ]
    def __str__(self):
        return '过滤ST股票'

class Filter_growth_is_down(Filter_stock_list):
    __name__='Filter_growth_is_down'
    def __init__(self,params):
        self.day_count = params.get('day_count', 20)
    
    def update_params(self,context,params):
        self.day_count = params.get('day_count', self.day_count)

    def filter(self,context,data,stock_list):
        return [stock for stock in stock_list if get_growth_rate(stock, self.day_count) > 0]
    
    def __str__(self):
        return '过滤n日增长率为负的股票'
 
class Filter_blacklist(Filter_stock_list):
    __name__='Index28_condition'
    def __get_blacklist(self):
        # 黑名单一览表，更新时间 2016.7.10 by 沙米
        # 科恒股份、太空板业，一旦2016年继续亏损，直接面临暂停上市风险
        blacklist = ["600656.XSHG", "300372.XSHE", "600403.XSHG", "600421.XSHG", "600733.XSHG", "300399.XSHE",
                     "600145.XSHG", "002679.XSHE", "000020.XSHE", "002330.XSHE", "300117.XSHE", "300135.XSHE",
                     "002566.XSHE", "002119.XSHE", "300208.XSHE", "002237.XSHE", "002608.XSHE", "000691.XSHE",
                     "002694.XSHE", "002715.XSHE", "002211.XSHE", "000788.XSHE", "300380.XSHE", "300028.XSHE",
                     "000668.XSHE", "300033.XSHE", "300126.XSHE", "300340.XSHE", "300344.XSHE", "002473.XSHE"]
        return blacklist
        
    def filter(self,context,data,stock_list):
        blacklist = self.__get_blacklist()
        return [stock for stock in stock_list if stock not in blacklist]
    def __str__(self):
        return '过滤黑名单股票'
        
class Filter_rank(Filter_stock_list):
    __name__='Filter_rank'
    def __init__(self,params):
        self.rank_stock_count = params.get('rank_stock_count',20)
    def update_params(self,context,params):
        self.rank_stock_count = params.get('self.rank_stock_count', self.rank_stock_count)

    def before_trading_start(self, context):

        if len(context.stock_list) > self.rank_stock_count:
            context.stock_list = context.stock_list[:self.rank_stock_count]

        return None

    def filter(self,context,data,stock_list):
        return None
        
    def __str__(self):
        return '股票评分排序 [评分股数: %d ]'%(self.rank_stock_count)
        
class Filter_buy_count(Filter_stock_list):
    __name__='Filter_buy_count'
    def __init__(self,params):
        self.buy_count = params.get('buy_count',3)

    def update_params(self,context,params):
        self.buy_count = params.get('buy_count', self.buy_count)

    def before_trading_start(self, context):

        if len(context.stock_list) > self.buy_count:
            context.stock_list = context.stock_list[:self.buy_count]

        return context.stock_list

    def filter(self,context,data,stock_list):

        if len(context.stock_list) > self.buy_count:
            context.stock_list = context.stock_list[:self.buy_count]

        return context.stock_list
    def __str__(self):
        return '获取最终待购买股票数:[ %d ]'%(self.buy_count)