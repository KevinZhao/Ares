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

	#更新当日持仓金额
	def update_amount(self, no):

		code = no_to_code(no)
		
		#选出持仓金额 = 0 的股票代码
		resultProxy= _db.execute(
		    text('select * from tb_daily where no = :no and amount = :amount order by date DESC'), {'no':no, 'amount': 0})
		result_list = resultProxy.fetchall()
		
		amount = 0

		for result in result_list:
		
			date = result[7]
			volume = result[3]
			#print('date, volume', date, volume)
			
			#检索tb_history_price该日收盘价格
			resultProxy_price = _db.execute(
				text('select close from tb_history_price where code = :code and date = :date'), {'code':code, 'date':date})
			close = resultProxy_price.fetchall()

			#print('close', close)
			
			if (len(close) == 1):
				close = close[0][0]
				amount = volume * close
				#print(amount)

			#当日无收盘价格，停盘状态
			else:    
				#检索停牌前市值总额
				resultProxy= _db.execute(
				    text('select * from tb_daily where no = :no and amount != 0 and date < :date order by date DESC'), {'no':no, 'date': date})
				result_list = resultProxy.fetchone()
				
				#有数据且持股数相等，则用停牌前最后一天的市值
				if result_list != None and result_list[3] == volume:
				    amount = result_list[4]
				
				#无数据，则检索历史价格，检索停牌前最后一天的价格
				if amount == 0:
					resultProxy_price = _db.execute(
				    	text('select * from tb_history_price where code = :code and date < :date order by date DESC'), {'code':code, 'date': date})
					result_price = resultProxy_price.fetchone()
				    
					if result_price != None:
						close = result_price[3]
						amount = volume * close

					else:
						print('there is something wrong with amount ', code, date)

			#更新数据库
			if amount != 0:
				update_db = _db.execute(
					text('update tb_daily set Amount = :amount where no =:no and date=:date'),{'amount':amount,'no':no, 'date':date})

		pass

	#更新当日持仓金额
	def update_amount_diff(self, no):

		code = no_to_code(no)
		
		#选出持仓金额 = 0 的股票代码
		resultProxy= _db.execute(
			text('select * from tb_daily where no = :no and amount_diff is null order by date DESC'), {'no':no})
		result_list = resultProxy.fetchall()

		result_count = len(result_list)
		for i in range(0, result_count - 1):
		
			amount_diff = None
			date = result_list[i][7]
			volume_current = result_list[i][3]

			#上一个交易日持仓数
			resultProxy_previous= _db.execute(
				text('select * from tb_daily where no = :no and date <:date order by date DESC'), {'no':no, 'date':date})
			result_previous_list = resultProxy_previous.fetchall()

			volume_previous = result_previous_list[0][3]
			volume_diff = volume_current - volume_previous

			#print('date, volume_current, volume_previous, volume_diff', date, volume_current, volume_previous, volume_diff)
			
			#持股差值为0，增量为0
			if volume_diff == 0:
				amount_diff = 0

			#持股差值不为0，则
			else:
				#检索tb_history_price该日收盘价格
				resultProxy_price = _db.execute(
					text('select close from tb_history_price where code = :code and date = :date'), {'code':code, 'date':date})
				close = resultProxy_price.fetchall()

				#当日有收盘价格，交易状态
				if (len(close) == 1):
					close = close[0][0]
					amount_diff = volume_diff * close

					#print(close, amount_diff)

				#当日无收盘价格，停盘状态
				else:    
					#检索停牌前持股数
					resultProxy= _db.execute(
					    text('select * from tb_daily where no = :no and amount_diff is not null and date < :date order by date DESC'), {'no':no, 'date': date})
					volume_result_list = resultProxy.fetchone()
					
					#有数据且持股数相等，则增量为0，#todo delete this one
					if volume_result_list != None and volume_result_list[3] == volume_current:
					    amount_diff = 0

					#停牌期间送股
					if amount_diff == None:
						print('there is something wrong with amount diff, ', code, date)
			
			#更新数据库
			if amount_diff != None:
				update_db = _db.execute(
					text('update tb_daily set Amount_diff = :amount_diff where no =:no and date=:date'),{'amount_diff':amount_diff,'no':no, 'date':date})

		pass

	#全部更新
	def update_HK_detail(self):

		#清空价格数据
		self.delete_history_data()

		#港股通代码转换
		resultProxy = _db.execute(text('select distinct no from tb_daily'))
		no_result = resultProxy.fetchall()
		result_array= []

		ts_adatper = Tushare_adapter()

		#循环遍历求差值
		for no in no_result:

			code = no_to_code(no[0])
		    
			ts_adatper.update_history_price(code)

			print('processing code:', no[0])
			self.update_amount(no[0])
			self.update_amount_diff(no[0])

		pass

	#删除历史价格表
	def delete_history_data(self):
	    
		resultProxy = _db.execute(text('TRUNCATE tb_history_price'))
		print('all history price had been removed')

		pass

