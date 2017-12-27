# -*- coding:utf-8 -*- 
from src.util import *

import numpy as np
import pandas as pd
import urllib.request
import time
import datetime
import re
import os
import os.path
import shutil

from src.HK_data_processing import *
from src.Tushare_adapter import *

class HK_data_processing():
	_name = 'HK_data_processing'
	_db = Jiatou_DB()._db
	_tb_daily = {
				'id':0, 
				'No':1,
				'Name':2,
				'Volume':3,
				'Percent':4,
				'Amount_diff':5,
				'Amount':6,
				'Date':7}

	_tb_histoy_price = {
				'date':0,
				'open':1,
				'close':2,
				'high':3,
				'low':4,
				'volume':5,
				'code':6
				}

	#更新当日持仓金额
	def update_amount(self, no):

		code = no_to_code(no)
		
		#选出持仓金额需要更新(amount is null) 的股票代码
		resultProxy= self._db.execute(
		    text('select * from tb_daily where no = :no and amount is null order by date DESC'), {'no':no})
		result_list = resultProxy.fetchall()
		
		amount = None

		for result in result_list:
		
			date = result[self._tb_daily['Date']]
			volume = result[self._tb_daily['Volume']]
			
			#检索tb_history_price该日收盘价格
			resultProxy_price = self._db.execute(
				text('select * from tb_history_price where code = :code and date = :date'), {'code':code, 'date':date})
			close = resultProxy_price.fetchall()

			#该交易日如果有数据，交易状态
			if (len(close) != 0):
				close = close[0][self._tb_histoy_price['close']]
				amount = volume * close

			#该交易日如没有数据，停盘状态
			else:    
				#检索停牌前港资市值总额
				resultProxy= self._db.execute(
				    text('select * from tb_daily where no = :no and amount != 0 and date < :date order by date DESC'), {'no':no, 'date': date})
				result_list = resultProxy.fetchone()
				
				#港资持仓有数据
				if result_list != None:

					#持股数相等，则用停牌前最后一天的市值
					if result_list[self._tb_daily['Volume']] == volume:
						amount = result_list[self._tb_daily['Amount']]

				    #持股数不等，可能：停牌期间送转
					else:
						amount = 0
						print('implementation not completed, error 001', code, date)

				#港资持仓无数据
				else:
					#则检索历史价格，检索停牌前最后一天的价格
					resultProxy_price = self._db.execute(
						text('select * from tb_history_price where code = :code and date < :date order by date DESC'), {'code':code, 'date': date})
					result_price = resultProxy_price.fetchone()

					#有历史交易数据
					if result_price != None:
						close = result_price[self._tb_histoy_price['close']]
						amount = volume * close

					#无历史交易数据
					else:
						amount = 0
						print('implementation not completed, error 002', code, date)

			#更新数据库
			if amount != None:
				update_db = self._db.execute(
					text('update tb_daily set Amount = :amount where no =:no and date=:date'),{'amount':amount,'no':no, 'date':date})

		pass

	#更新当日持仓金额
	def update_amount_diff(self, no):

		code = no_to_code(no)
		
		#选出需要更新(amount is null) 的股票代码
		resultProxy= self._db.execute(
			text('select * from tb_daily where no = :no and amount_diff is null order by date DESC'), {'no':no})
		result_list = resultProxy.fetchall()

		#遍历更新Amount_diff
		for result in result_list:
		
			amount_diff = None

			#当前交易日持仓股数
			date = result[self._tb_daily['Date']]
			volume_current = result[self._tb_daily['Volume']]

			#历史上一个交易日持仓数
			resultProxy_previous= self._db.execute(
				text('select * from tb_daily where no = :no and date <:date order by date DESC'), {'no':no, 'date':date})
			result_previous_list = resultProxy_previous.fetchall()

			#存在历史上一个交易日数据
			if len(result_previous_list) != 0:
				volume_previous = result_previous_list[0][self._tb_daily['Volume']]
			
				#持仓数差额
				volume_diff = volume_current - volume_previous

			#不存在历史上一个交易日数据，新建仓，2017年3月17日
			else:
				volume_diff = volume_current
			
			#持股差值为0，增量为0
			if volume_diff == 0:
				amount_diff = 0

			#持股差值不为0，则
			else:
				#检索tb_history_price该日收盘价格
				resultProxy_price = self._db.execute(
					text('select * from tb_history_price where code = :code and date = :date'), {'code':code, 'date':date})
				close = resultProxy_price.fetchall()

				#当日有收盘价格，交易状态
				if (len(close) != 0):
					close = close[0][self._tb_histoy_price['close']]
					amount_diff = volume_diff * close

				#当日无收盘价格，停盘状态
				else:    
					amount_diff = 0

			#更新数据库
			if amount_diff != None:
				update_db = self._db.execute(
					text('update tb_daily set Amount_diff = :amount_diff where no =:no and date=:date'),{'amount_diff':amount_diff,'no':no, 'date':date})
		pass

	#全部更新
	def update_HK_detail(self):

		print('starting update_HK_detail')

		#清空价格数据
		self.delete_history_data()

		#港股通代码转换
		resultProxy = self._db.execute(text('select distinct no from tb_daily'))
		no_result = resultProxy.fetchall()
		result_array= []

		ts_adatper = Tushare_adapter()

		#循环遍历求差值
		for no in no_result:

			code = no_to_code(no[0])
		    
			ts_adatper.update_history_price(code)
			
			self.update_amount(no[0])
			self.update_amount_diff(no[0])

		print('update_HK_detail is done')

		pass

	#删除历史价格表
	def delete_history_data(self):
	    
		resultProxy = self._db.execute(text('TRUNCATE tb_history_price'))
		print('all history price had been removed')

		pass

