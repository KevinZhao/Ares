# -*- coding:utf-8 -*- 

from kuanke.user_space_api import *
from rule import *
import talib
import pandas as pd 
import numpy as np

''' ----------------------实盘类----------------------------'''
class Trader(Rule):
    def __init__(self,params):
        self.trader_client = params.get('client','xzsec')
        self.trader_baseurl = params.get('url', 'http://sohunjug.com:8001/')
        self.username = params.get('username','')
        self.password = params.get('password', '')
        self.s = None
        self.count = 0
        self.positions = {}

    def after_adjust_end(self,context,data):
        print('实盘同步开始')
        self.context = context
        send_message('实盘同步开始')
        try:
            diff_positions = True
            while diff_positions:
                diff_positions = self.diff(self.get_positions(), context.portfolio.positions)
                if diff_positions:
                    print('卖出 ' + ' '.join(diff_positions))
                    send_message('卖出 ' + ' '.join(diff_positions))
                    self.close_list(diff_positions)
                diff_positions = self.diff(context.portfolio.positions, self.get_positions())
                if diff_positions:
                    print('买入 ' + ' '.join(diff_positions))
                    send_message('买入 ' + ' '.join(diff_positions))
                    self.open_list(diff_positions)

            send_message('同步后持仓: ' + ' '.join(self.get_positions()))
            print('同步后持仓: ' + json.dumps(self.get_positions()))
        except Exception as e:
            print(e)

    def diff(self, from_positions, to_positions):
        fs = list(x for x in from_positions.keys())
        ts = list(x for x in to_positions.keys())
        f = list(x[:6] for x in from_positions.keys())
        t = list(x[:6] for x in to_positions.keys())
        flag = True if len(fs) > 0 and fs[0] == f[0] else False
        ds = list(x for x in f if x not in t)
        de = {}
        if flag:
            for stock in ds:
                if stock in from_positions:
                    if from_positions[stock]['Amount'] > 0:
                        if stock in to_positions:
                            if from_positions[stock]['Amount'] != to_positions[stock].total_amount:
                                from_positions[stock]['Amount'] -= to_positions[stock].total_amount
                                de[stock[:6]] = from_positions[stock]
                        else:
                            de[stock[:6]] = from_positions[stock]
        else:
            for key in f:
                stock = None
                for st in fs:
                    if key in st:
                        stock = st
                if stock in from_positions:
                    if from_positions[stock].total_amount > 0:
                        if stock[:6] in to_positions:
                            if from_positions[stock].total_amount != to_positions[stock[:6]]['Amount']:
                                from_positions[stock].total_amount -= to_positions[stock[:6]]['Amount']
                                de[key] = from_positions[stock]
                        else:
                            de[key] = from_positions[stock]

        return de if len(de) > 0 else False

    def close_list(self, stocks):
        if not stocks: return
        if not self.get_session(): return
        flag = True
        while flag:
            flag = False
            for stock in stocks:
                url = self.trader_baseurl + 'trade'
                p = dict(
                    code = stock,
                    price = stocks[stock]['NowPrice'] - 0.02,
                    amount = stocks[stock]['Amount'],
                    type = 's'
                )
                if self.context.run_params.type == 'full_backtest':
                    del self.positions[stock[:6]]
                else:
                    r = self.s.post(url, data=json.dumps(p))
            positions = self.get_positions()
            for stock in stocks:
                if stock in positions:
                    if positions[stock]['Amount'] > 0:
                        flag = True
            sleep(1)
            self.revoke()

    def revoke(self):
        if self.context.run_params.type == 'full_backtest': return
        if not self.get_session(): return
        url = self.trader_baseurl + 'revokelist'
        positions = json.loads(str(self.s.get(url).text))
        for stock in positions:
            url = self.trader_baseurl + 'revokeorder'
            p = dict(
                order_id = stock['RevokeID']
            )
            r = self.s.post(url, data=json.dumps(p))

    def open_list(self, stocks):
        if not stocks: return
        if not self.get_session(): return
        count = 1
        flag = True
        while flag:
            flag = False
            for stock in stocks:
                url = self.trader_baseurl + 'trade'
                p = dict(
                    code = stock,
                    price = stocks[stock].price + count * 0.02,
                    amount = stocks[stock].total_amount,
                    type = 'b'
                )
                if self.context.run_params.type == 'full_backtest':
                    one = {}
                    one['Code'] = stock[:6]
                    one['Price'] = stocks[stock].price
                    one['NowPrice'] = stocks[stock].price
                    one['Amount'] = stocks[stock].total_amount
                    self.positions[stock[:6]] = one
                else:
                    r = self.s.post(url, data=json.dumps(p))
            positions = self.get_positions()
            for stock in stocks:
                if stock[:6] not in positions:
                    flag = True
                    count += 1
            sleep(1)
            self.revoke()

    def get_positions(self):
        if not self.get_session(): return
        url = self.trader_baseurl + 'list'
        if self.context.run_params.type == 'full_backtest':
            return self.positions
        return json.loads(str(self.s.get(url).text.encode('utf-8')))

    def get_session(self):
        if self.context.run_params.type == 'full_backtest': return True
        if self.count > 10: return False
        flag = True
        if self.s is not None:
            url = self.trader_baseurl + 'account'
            r = self.s.get(url)
            if 'Crash' in r.text:
                flag = False
        if flag:
            self.s = requests.session()
            p = dict(
                username = self.username,
                password = self.password
            )
            url = self.trader_baseurl + 'login'
            r = self.s.post(url, data=json.dumps(p))
            if 'token' not in r.text:
                self.count += 1
                return False
            self.s.headers['Authorization'] = json.loads(r.text)['token']
        return True
    def __str__(self):
        return '策略实盘跟单'
