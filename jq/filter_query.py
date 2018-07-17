# -*- coding:utf-8 -*- 

from kuanke.user_space_api import *
from rule import *

'''==============================选股 query过滤器基类=============================='''
class Filter_query(Rule):
    def filter(self,context,data,q):
        return None

'''------------------小市值选股器-----------------'''
class Pick_small_circulating_market_cap(Filter_query):
    def filter(self,context,data,q):
        return query(valuation).order_by(valuation.market_cap.asc())
    def __str__(self):
        return '按市值倒序选取股票'

class Filter_circulating_market_cap(Filter_query):
    __name__='Filter_market_cap'
    def __init__(self,params):
        self.cm_cap_min = params.get('cm_cap_min',0)
        self.cm_cap_max = params.get('cm_cap_max',100)
    def update_params(self,context,params):
        self.cm_cap_min = params.get('cm_cap_min',self.cm_cap_min)
        self.cm_cap_max = params.get('cm_cap_max',self.cm_cap_max)
    def filter(self,context,data,q):
        return q.filter(
            valuation.circulating_market_cap <= self.cm_cap_max,
            valuation.circulating_market_cap >= self.cm_cap_min
            )
    def __str__(self):
        return '根据流通市值范围选取股票： [ %d < circulating_market_cap < %d]'%(self.cm_cap_min,self.cm_cap_max) 


class Filter_limite(Filter_query):
    def __init__(self,params):
        self.pick_stock_count = params.get('pick_stock_count',0)
    def update_params(self,context,params):
        self.pick_stock_count = params.get('pick_stock_count',self.pick_stock_count)
    def filter(self,context,data,q):
        return q.limit(self.pick_stock_count)
    def __str__(self):
        return '初选股票数量: %d'%(self.pick_stock_count)