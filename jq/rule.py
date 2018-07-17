# -*- coding:utf-8 -*- 

from kuanke.user_space_api import *

''' ==============================规则基类================================'''
class Rule(object):
    # 持仓操作的事件
    on_open_position = None # 买股调用外部函数
    on_close_position = None # 卖股调用外部函数
    on_clear_position = None # 清仓调用外部函数
    on_get_obj_by_class_type = None # 通过类的类型查找已创建的类的实例
    memo = ''   # 对象简要说明

    def __init__(self,params):
        pass
    def initialize(self,context):
        pass
    def handle_data(self,context, data):
        pass
    def before_trading_start(self,context):
        pass
    def after_trading_end(self,context):
        pass
    def process_initialize(self,context):
        pass
    def after_code_changed(self,context):
        initialize(context)
        pass
    # 卖出股票时调用的函数
    # price为当前价，amount为发生的股票数,is_normail正常规则卖出为True，止损卖出为False
    def when_sell_stock(self,position,order,is_normal):
        pass
    # 买入股票时调用的函数
    # price为当前价，amount为发生的股票数
    def when_buy_stock(self,stock,order):
        pass
    # 清仓时调用的函数
    def when_clear_position(self,context):
        pass
    # 调仓前调用
    def before_adjust_start(self,context,data):
        pass
    # 调仓后调用用
    def after_adjust_end(self,context,data):
        pass
    # 更改参数
    def update_params(self,context,params):
        pass

    # 持仓操作事件的简单判断处理，方便使用。
    def open_position(self,security, value):
        if self.on_open_position != None:
            return self.on_open_position(self,security,value)
    def close_position(self,position,is_normal = True):
        if self.on_close_position != None:
            return self.on_close_position(self,position,is_normal = True)
    def clear_position(self,context):
        if self.on_clear_position != None:
            self.on_clear_position(self,context)
    # 通过类的类型获取已创建的类的实例对象
    # 示例 obj = get_obj_by_class_type(Index28_condition)
    def get_obj_by_class_type(self,class_type):
        if self.on_get_obj_by_class_type != None:
            return self.on_get_obj_by_class_type(class_type)
        else:
            return None
    # 为日志显示带上是哪个规则器输出的
    def log_info(self,msg):
        log.info('%s: %s'%(self.memo,msg))
    def log_warn(self,msg):
        log.warn('%s: %s'%(self.memo,msg))
    def log_debug(self,msg):
        log.debug('%s: %s'%(self.memo,msg))
