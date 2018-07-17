# -*- coding:utf-8 -*- 

from kuanke.user_space_api import *
from rule import *
from util import *
import pandas as pd

'''==============================调仓条件判断器基类=============================='''
class Adjust_condition(Rule):
    __name__='Adjust_condition'

    @property
    def can_adjust(self):
        return True

'''-------------------------MACD指数涨幅调仓判断器----------------------'''
class Index_MACD_condition(Adjust_condition):
    __name__='Index_MACD_condition'
    def __init__(self,params):
        self.index = params.get('index','')
        self.t_can_adjust = False
    
    def update_params(self,context,params):
        self.index = params.get('index',self.index)
        
    @property    
    def can_adjust(self):
        return self.t_can_adjust

    def handle_data(self,context, data):

        if context.timedelt % 60 != 1:
            return

        #金叉
        if context.index_df.iloc[-1]['macd'] > 0 and context.index_df.iloc[-2]['macd'] < 0:
            context.position = 0.8
        #死叉
        if context.index_df.iloc[-1]['macd'] < 0 and context.index_df.iloc[-2]['macd'] > 0:
            context.position = 0

        #低位 金叉之前
        if context.index_df.iloc[-1]['diff'] < 0 and context.index_df.iloc[-2]['diff']< 0 and context.index_df.iloc[-1]['macd'] < 0:
            #diff上拐
            if context.index_df.iloc[-1]['diff'] > context.index_df.iloc[-2]['diff'] and (context.index_df.iloc[-1]['diff']/context.index_df.iloc[-2]['diff']) < 1.005:
                #macd绿脚线缩短
                if context.index_df.iloc[-1]['macd'] > context.index_df.iloc[-2]['macd']:
                    context.position = 0.6

            #diff下拐
            if context.index_df.iloc[-1]['diff'] < context.index_df.iloc[-2]['diff'] and (context.index_df.iloc[-1]['diff']/context.index_df.iloc[-2]['diff']) > 1.005:
                #macd绿脚线加长
                if context.index_df.iloc[-1]['macd'] < context.index_df.iloc[-2]['macd']:
                    context.position = 0

        #低位 金叉之后
        if context.index_df.iloc[-1]['diff'] < 0 and context.index_df.iloc[-2]['diff']< 0 and context.index_df.iloc[-1]['macd'] > 0:
            #diff上拐
            if context.index_df.iloc[-1]['diff'] > context.index_df.iloc[-2]['diff'] and (context.index_df.iloc[-1]['diff']/context.index_df.iloc[-2]['diff']) < 1.005:
                #macd绿脚线缩短
                if context.index_df.iloc[-1]['macd'] > context.index_df.iloc[-2]['macd']:
                    context.position = 1.0

            #diff下拐
            if context.index_df.iloc[-1]['diff'] < context.index_df.iloc[-2]['diff'] and (context.index_df.iloc[-1]['diff']/context.index_df.iloc[-2]['diff']) > 1.005:
                #macd绿脚线加长
                if context.index_df.iloc[-1]['macd'] < context.index_df.iloc[-2]['macd']:
                    context.position = 0.8


        #高位 死叉之前
        if context.index_df.iloc[-1]['diff'] > 0 and context.index_df.iloc[-2]['diff'] > 0 and context.index_df.iloc[-1]['macd'] > 0:
            #diff下拐
            if context.index_df.iloc[-1]['diff'] < context.index_df.iloc[-2]['diff'] and (context.index_df.iloc[-1]['diff']/context.index_df.iloc[-2]['diff']) < 0.995:
                #macd红脚线缩短
                if context.index_df.iloc[-1]['macd'] < context.index_df.iloc[-2]['macd']:
                    context.position = 0.8

            #diff上拐
            if context.index_df.iloc[-1]['diff'] > context.index_df.iloc[-2]['diff'] and (context.index_df.iloc[-1]['diff']/context.index_df.iloc[-2]['diff']) > 1.005:
                #macd红脚线加长
                if context.index_df.iloc[-1]['macd'] > context.index_df.iloc[-2]['macd']:
                    context.position = 1

        #高位 死叉之后
        if context.index_df.iloc[-1]['diff'] > 0 and context.index_df.iloc[-2]['diff'] > 0 and context.index_df.iloc[-1]['macd'] < 0:
            #diff下拐
            if context.index_df.iloc[-1]['diff'] < context.index_df.iloc[-2]['diff'] and (context.index_df.iloc[-1]['diff']/context.index_df.iloc[-2]['diff']) < 0.995:
                #macd红脚线缩短
                if context.index_df.iloc[-1]['macd'] < context.index_df.iloc[-2]['macd']:
                    context.position = 0

            #diff上拐
            if context.index_df.iloc[-1]['diff'] > context.index_df.iloc[-2]['diff'] and (context.index_df.iloc[-1]['diff']/context.index_df.iloc[-2]['diff']) > 1.005:
                #macd红脚线加长
                if context.index_df.iloc[-1]['macd'] > context.index_df.iloc[-2]['macd']:
                    context.position = 0.4

        if context.position > 0:
            self.t_can_adjust = True
        else:
            #self.clear_position(context)
            self.t_can_adjust = False

        print('当前仓位：=', context.position)

        return self.t_can_adjust
    
    def before_trading_start(self,context):
        
        pass


