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
from decimal import Decimal

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
		resultProxy_no = self._db.execute(
		    text('select * from tb_daily where no = :no and amount is null order by date DESC'), {'no':no})
		result_list = resultProxy_no.fetchall()

		resultProxy_no.close()
		
		amount = None

		for result in result_list:
		
			date = result[self._tb_daily['Date']]
			volume = result[self._tb_daily['Volume']]

			#检索tb_history_price该日收盘价格
			resultProxy_price = self._db.execute(
				text('select * from tb_history_price where code = :code and date = :date'), {'code':code, 'date':date})
			close = resultProxy_price.fetchall()

			resultProxy_price.close()

			#该交易日如果有数据，交易状态
			if (len(close) != 0):
				close = close[0][self._tb_histoy_price['close']]
				amount = volume * close

			#该交易日如没有数据，停盘状态
			else:    
				#检索停牌前港资市值总额
				resultProxy_last= self._db.execute(
				    text('select * from tb_daily where no = :no and amount != 0 and date < :date order by date DESC'), {'no':no, 'date': date})
				result_last = resultProxy_last.fetchone()
				resultProxy_last.close()
				
				#港资持仓有数据
				if result_last != None:

					#持股数相等，则用停牌前最后一天的市值
					if result_last[self._tb_daily['Volume']] == volume:
						amount = result_last[self._tb_daily['Amount']]

				    #持股数不等，可能：停牌期间送转
					else:
						amount = 0

				#港资持仓无数据
				else:

					#则检索历史价格，检索停牌前最后一天的价格
					resultProxy_price = self._db.execute(
						text('select * from tb_history_price where code = :code and date < :date order by date DESC'), {'code':code, 'date': date})
					result_price = resultProxy_price.fetchone()
					resultProxy_price.close()

					#有历史交易数据
					if result_price != None:
						close = result_price[self._tb_histoy_price['close']]
						amount = volume * close

					#无历史交易数据
					else:
						amount = 0

			#更新数据库
			if amount != None:
				update_db = self._db.execute(
					text('update tb_daily set Amount = :amount where no =:no and date=:date'),{'amount':amount,'no':no, 'date':date})
				update_db.close()

		pass

	#更新当日持仓金额
	def update_amount_diff(self, no):

		code = no_to_code(no)
		
		#选出需要更新的股票代码
		resultProxy= self._db.execute(
			text('select * from tb_daily where no = :no and amount_diff is null order by date DESC'), {'no':no})
		result_list = resultProxy.fetchall()
		resultProxy.close()

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
			resultProxy_previous.close()

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
				resultProxy_price.close()

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
				update_db.close()
		pass

	#全部更新
	def update_holding_detail(self):

		log('starting update_holding_detail')

		#港股通代码转换
		resultProxy = self._db.execute(text('select distinct no from tb_daily where amount_diff is NULL'))
		no_result = resultProxy.fetchall()
		resultProxy.close()

		if no_result == []:
			return

		ts_adatper = Tushare_adapter()

		#循环遍历求差值
		for no in no_result:

			code = no_to_code(no[0])
		    
			ts_adatper.update_history_price(code)
			
			self.update_amount(no[0])
			self.update_amount_diff(no[0])
		pass

		log('update_holding_detail is done')

		pass

	#---5/10/15/20 交易日持股变化----------------------------------------#
	def update_period(self, period):

		db = Jiatou_DB()._db

		#交易日期
		resultProxy = self._db.execute(text('select distinct date from tb_daily order by date DESC'))
		day_result = resultProxy.fetchall()

		#计算起始日期
		day_end = day_result[0][0]
		day_start = day_result[period -1][0]

		#选取所有股票代码
		resultProxy = self._db.execute(text('select distinct no from tb_daily'))
		no_result = resultProxy.fetchall()

		#数据表格式
		tb_period_stat_df = pd.DataFrame(columns = ['No', 'Name', 'Percent_diff', 'Amount_diff', 'Period', 'Day_end'])

		#遍历循环更新数据
		for no in no_result:

			#区间内，持股数变化 volume, 持股比例变化 percent, 金额变化 Amount_diff的累积
			code = no_to_code(no[0])
			name = code_to_name(code)

			percent_start = 0
			percent_end = 0

			#区间内数据集结果
			resultProxy = self._db.execute(text(
				'select * from tb_daily where no =:no and date >= :day_start and date <= :day_end order by date DESC'),
				{'no':no[0], 'day_start':day_start, 'day_end':day_end})

			tb_daily_result = resultProxy.fetchall()

			if len(tb_daily_result) > 0:


				percent_start = tb_daily_result[len(tb_daily_result) - 1][self._tb_daily['Percent']]
				percent_end = tb_daily_result[0][self._tb_daily['Percent']]

				percent_diff = percent_end - percent_start
				percent_diff = Decimal(percent_diff).quantize(Decimal('0.00'))

				amount_diff = 0
				for i in range(0, len(tb_daily_result)):
					print(tb_daily_result[i][self._tb_daily['Amount_diff']])
					amount_diff = amount_diff + tb_daily_result[i][self._tb_daily['Amount_diff']]

				amount_diff = Decimal(amount_diff).quantize(Decimal('0.00'))
 
				record = []
				record.append(code)
				record.append(name)
				record.append(percent_diff)
				record.append(amount_diff)
				record.append(period)
				record.append(day_end)

				tb_period_stat_df.loc[tb_period_stat_df.shape[0]+1] = record

			pass
			
		tb_period_stat_df.to_sql('tb_period_stat', con=db, if_exists='append', index=False)

		pass


	def update_trend_history(self, start_date):

		resultProxy = self._db.execute(text('select distinct date from tb_daily where date >= :date order by date DESC'),
			{'date':start_date})
		day_result = resultProxy.fetchall()

		for date in day_result:
			self.update_trend(date[0])

		pass

	def update_trend_last_day(self):

		resultProxy = self._db.execute(text('select distinct date from tb_daily order by date DESC'))
		day_result = resultProxy.fetchall()

		self.update_trend(day_result[0][0])


	#---持股历史高位筛选----------------------------------------#
	def update_trend(self, currentDate = '2018-05-24'):

		db = Jiatou_DB()._db
		tb_trend_df = pd.DataFrame(columns = 
			['code', 'name', 'percent', 'max_percent', 'quantile', 
			'amount_diff', 'percent_diff',
			'amount_diff_5', 'percent_diff_5',
			'amount_diff_10', 'percent_diff_10',
			'amount_diff_20', 'percent_diff_20',
			'date'])

		period_list = [1, 5, 10, 20]

		#选取所有股票代码
		resultProxy = self._db.execute(text('select distinct no from tb_daily'))
		no_result = resultProxy.fetchall()
		resultProxy.close()

		#交易日期
		resultProxy = self._db.execute(text(
			'select distinct date from tb_daily where date <=:date order by date DESC'),
			{'date':currentDate})
		day_result = resultProxy.fetchall()
		resultProxy.close()

		date_length = len(day_result)

		#计算起始日期
		if date_length >= 21:
			day_start = day_result[20][0]
		else:
			day_start = day_result[date_length - 1][0]

		for no in no_result:

			print(no)

			code = no_to_code(no[0])
			name = code_to_name(code)

			#历史新高更好还是30日或60日新高更好
			resultProxy = self._db.execute(text(
				'select max(percent) from tb_daily where no =:no and date <=:day_end'),
				{'no':no[0], 'day_end':currentDate})

			result = resultProxy.fetchall()
			resultProxy.close()

			max_percent = result[0][0]

			if max_percent == None:
				continue

			max_percent = Decimal(max_percent).quantize(Decimal('0.00'))

			if max_percent == 0:
				continue

			#区间内数据集结果
			resultProxy = self._db.execute(text(
				'select * from tb_daily where no =:no and date >= :day_start and date <= :day_end order by date DESC'),
				{'no':no[0], 'day_start':day_start, 'day_end':currentDate})

			result = resultProxy.fetchall()
			resultProxy.close()

			if result != []:

				percent = result[0][self._tb_daily['Percent']]
				percent = Decimal(percent).quantize(Decimal('0.00'))

				amount_diff = result[0][self._tb_daily['Amount_diff']]/1000000
				amount_diff = Decimal(amount_diff).quantize(Decimal('0.00'))

				#percent_diff

				if len(result) > 1:
					percent_diff = result[0][self._tb_daily['Percent']] - result[1][self._tb_daily['Percent']]
					percent_diff = Decimal(percent_diff).quantize(Decimal('0.00'))
				else:
					percent_diff = 0.00

				percent_diff_5 = 0.00
				amount_diff_5 = 0.00
				if len(result) > 5:
					percent_diff_5 = result[0][self._tb_daily['Percent']] - result[5][self._tb_daily['Percent']]
					percent_diff_5 = Decimal(percent_diff_5).quantize(Decimal('0.00'))

					amount_diff_5 = 0.00
					#print(name)
					for i in range(0, 5):
						amount_diff_5 = amount_diff_5 + result[i][self._tb_daily['Amount_diff']]
						pass
					amount_diff_5 = amount_diff_5/1000000
					amount_diff_5 = Decimal(amount_diff_5).quantize(Decimal('0.00'))

				percent_diff_10 = 0.00
				amount_diff_10 = 0.00
				if len(result) > 10:
					percent_diff_10 = result[0][self._tb_daily['Percent']] - result[10][self._tb_daily['Percent']]
					percent_diff_10 = Decimal(percent_diff_10).quantize(Decimal('0.00'))

					for i in range(0, 10):
						amount_diff_10 = amount_diff_10 + result[i][self._tb_daily['Amount_diff']]
						pass
					amount_diff_10 = amount_diff_10/1000000
					amount_diff_10 = Decimal(amount_diff_10).quantize(Decimal('0.00'))

				percent_diff_20 = 0.00
				amount_diff_20 = 0.00
				if len(result) > 20:
					percent_diff_20 = result[0][self._tb_daily['Percent']] - result[20][self._tb_daily['Percent']]
					percent_diff_20 = Decimal(percent_diff_20).quantize(Decimal('0.00'))

					for i in range(0, 20):
						amount_diff_20 = amount_diff_20 + result[i][self._tb_daily['Amount_diff']]
						pass
					amount_diff_20 = amount_diff_20/1000000
					amount_diff_20 = Decimal(amount_diff_20).quantize(Decimal('0.00'))

			#持股比例低于1.0的不要
			if percent < 1.0:
				continue

			#
			if len(result) == 0:
				continue

			quantile = percent/max_percent
			quantile = Decimal(quantile).quantize(Decimal('0.00'))

			#PE-TTM数据
			#resultProxy = self._db.execute(text(
			#	'select * from tb_fundamental_info_lxr where stockCode =:no and date = :currentDate'),
			#	{'no':no[0], 'date':currentDate})
			#resultProxy.close()
			

			#当前值距离历史最高95%以上
			if quantile >= 0.95:
				record = []

				record.append(code)
				record.append(name)
				record.append(percent)
				record.append(max_percent)
				record.append(quantile)
				record.append(amount_diff)
				record.append(percent_diff)	
				record.append(amount_diff_5)			
				record.append(percent_diff_5)
				record.append(amount_diff_10)				
				record.append(percent_diff_10)
				record.append(amount_diff_20)				
				record.append(percent_diff_20)				
				record.append(currentDate)

				tb_trend_df.loc[tb_trend_df.shape[0]+1] = record

		tb_trend_df.to_sql('tb_trend', con=db, if_exists='append', index=False)

		print('done')

	#删除历史价格表
	def delete_history_data(self):
	    
		resultProxy = self._db.execute(text('delete from tb_history_price where 1'))
		log('all history price had been removed')

		pass

