# -*- coding:utf-8 -*- 
from sqlalchemy import create_engine
from src.util import *
import numpy as np
import pandas as pd
import time
import datetime
import re
import json

class Lixinger_adapter():
	_name = 'Lixinger_adapter'
	_db = Jiatou_DB()._db
	_token = 'df16b9ad-1ff5-4d3f-b295-83b6f2242589'
	_retry_count = 3
	_sleep_timer = 10

	''' ---------使用Lixinger_API更新fundamental-info基础信息-----------------------------------------'''
	def update_fundamental_info(self, code = "300450", startDate = "2018-05-14", endDate = "2018-06-08"):

		print('update_fundamental_info')

		url = "https://www.lixinger.com/api/open/a/stock/fundamental-info"
		metrics = [
			"pe_ttm", "pe_ttm_pos5", "pe_ttm_pos10", 
			"d_pe_ttm", "d_pe_ttm_pos5", "d_pe_ttm_pos10",
			"pb", "pb_pos5", "pb_pos10",
			"pb_wo_gw5", "pb_wo_gw5", "pb_wo_gw10",
			"ps_ttm", "ps_ttm_pos5", "ps_ttm_pos10",
			"dividend_r",
			"stock_price",
			"market_value",
			"share_holders_num",
			"t_market_value",
			"e_t_market_value",
			"e_t_market_value_p_sh",
			"securities_margin_trading",
			"smt_to_e_t_market_value_r",
			"ev_to_ebit",
			"earnings_yield"
		]

		parameters = {
			"token":self._token,
			"startDate":startDate,
			"endDate":endDate,
			"stockCodes":[code],
			"metrics":metrics
		}

		parameters_json = json.dumps(parameters, separators = (',',':'))

		result = url_request_with_json_retry(url, parameters_json, self._retry_count, self._sleep_timer)

		data = json.loads(result)

		df = pd.io.json.json_normalize(data)

		df.to_sql('tb_fundamental_info_lxr', con=self._db, if_exists='append', index=False)

		pass

	def update_fundamental_info_history(self):

		resultProxy = self._db.execute(
			text('select code from tb_basics'))
		result_list = resultProxy.fetchall()

		for code in result_list:

			print(code[0])
			#历史交易数据
			df = self.update_fundamental_info(code[0])

		pass


	''' ---------使用Lixinger_API更新fundamental-info基础信息-----------------------------------------'''
	def update_fs_info(self, code = "300450", startDate = "2017-01-01", endDate = "2018-05-14"):

		print('update_fs_info')

		url = "https://www.lixinger.com/api/open/a/stock/fs-info"

		period = "q"

		metrics = [
		#profit
		"profitStatement.toi", #营业总收入:   
		"profitStatement.oc", #营业支出:   
		"profitStatement.btaa", #营业税金及附加:   
		"profitStatement.se", #销售费用:   
		"profitStatement.me", #管理费用:   
		"profitStatement.fe", #财务费用:   
		"profitStatement.op", #营业利润:   
		"profitStatement.gp", #利润总额:   
		"profitStatement.ite", #所得税费用:   
		"profitStatement.np", #净利润:   
		"profitStatement.dnp", #归属于母公司股东的扣非净利润:    
		"profitStatement.npatootpc", #归属于母公司股东的净利润:   
		"profitStatement.mi0", #少数股东损益:   
		#balance
		"balanceSheet.ta", #资产总计:   
		"balanceSheet.tca", #流动资产合计:   
		"balanceSheet.tnca", #非流动资产合计:   
		"balanceSheet.tl", #负债合计:   
		"balanceSheet.tncl", #非流动负债合计:   
		"balanceSheet.toe", #所有者权益合计:   
		"balanceSheet.tseattpc", #归属于母公司的股东权益合计:   
		#cashflow
		"cashFlow.cifoa", #经营活动现金流入小计:    
		"cashFlow.oco", #经营活动现金流出小计:    
		"cashFlow.ncffoa", #经营活动产生的现金流量净额:    
		"cashFlow.cifia", #投资活动现金流入小计:    
		"cashFlow.cofia", #投资活动现金流出小计:
		"cashFlow.ncffia", #投资活动产生的现金流量净额:    
		"cashFlow.ciffa", #筹资活动现金流入小计:    
		"cashFlow.coffa", #筹资活动现金流出小计:    
		"cashFlow.ncfffa", #筹资活动产生的现金流量净额:    
		#metrics
		"metrics.wroe", #加权ROE:    
		"metrics.p_roe", #归属于母公司股东的ROE:    
		"metrics.d_p_roe", #归属于母公司股东的扣非ROE:    
		"metrics.roe", #净资产收益率(ROE):    
		"metrics.leverage", #杠杆比例:    
		"metrics.roa", #总资产收益率(ROA):    
		"metrics.asset_turnover", #资产周转率:    
		"metrics.np_sales_r", #净利润率:    
		"metrics.gross_profit_r", #毛利率(GM):    
		"metrics.roic", #ROIC:    
		"metrics.roc", #资本回报率(ROC):    
		"metrics.inventory_d_s", #存货周转天数:    
		"metrics.outstanding_d_s", #应收账款周转天数:    
		"metrics.advance_money_d_s", #预付账款周转天数:    
		"metrics.a_payable_d_s", #应付账款周转天数:    
		"metrics.d_received_d_s", #预收账款周转天数:    
		"metrics.money_d_s", #现金周转天数(CCC):    
		"metrics.fa_d_s", #固定资产周转天数:    
		"metrics.current_assets_d_s", #流动资产周转天数:    
		"metrics.owner_equity_d_s" #股东权益周转天数: 
		]

		metrics_items= [
		"wroe", #加权ROE:    
		"p_roe", #归属于母公司股东的ROE:    
		"d_p_roe", #归属于母公司股东的扣非ROE:    
		"roe", #净资产收益率(ROE):    
		"leverage", #杠杆比例:    
		"roa", #总资产收益率(ROA):    
		"asset_turnover", #资产周转率:    
		"np_sales_r", #净利润率:    
		"gross_profit_r", #毛利率(GM):    
		"roic", #ROIC:    
		"roc", #资本回报率(ROC):    
		"inventory_d_s", #存货周转天数:    
		"outstanding_d_s", #应收账款周转天数:    
		"advance_money_d_s", #预付账款周转天数:    
		"a_payable_d_s", #应付账款周转天数:    
		"d_received_d_s", #预收账款周转天数:    
		"money_d_s", #现金周转天数(CCC):    
		"fa_d_s", #固定资产周转天数:    
		"current_assets_d_s", #流动资产周转天数:    
		"owner_equity_d_s" #股东权益周转天数: 
		]

		sub_metrics = [
		"t", "t_y2y", "t_c2c", "c", "c_y2y", "c_c2c", "c_2y", "ttm", "ttm_y2y", "ttm_c2c"
		]

		for sub_item in sub_metrics:

			combine_metrics = []

			for item in metrics:
				combine_item = period + "." + item + "." + sub_item
				
				combine_metrics.append(combine_item)

				#print(item) 
				#break;

			parameters = {
				"token":self._token,
				"startDate":startDate,
				"endDate":endDate,
				"stockCodes":[code],
				"metrics":combine_metrics
			}

			parameters_json = json.dumps(parameters, separators = (',',':'))

			#print(parameters_json)

			result = url_request_with_json_retry(url, parameters_json, self._retry_count, self._sleep_timer)

			data = json.loads(result)

			for record in data:
				#print(record)

				stockCnName = record['stockCnName']
				stockCode = record['stockCode']
				date = record['date']

				q = record['q']

				metrics = q['metrics']
				cashFlow = q['cashFlow']
				balanceSheet = q['balanceSheet']
				profitStatement = q['profitStatement']

				for item in metrics:
					print(item)

					value = item.value

				break;

			#print(data)

			break;

		'''
		

		df = pd.io.json.json_normalize(data)

		df.to_sql('tb_fundamental_info_lxr', con=self._db, if_exists='append', index=False)
		'''

		pass


