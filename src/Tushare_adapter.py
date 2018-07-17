# -*- coding:utf-8 -*- 
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

from src.util import *

import numpy as np
import pandas as pd
import tushare as ts
import time
import datetime
import re

class Tushare_adapter():
	_name = 'Tushare_adapter'
	_db = Jiatou_DB()._db

	''' ---------使用tushare_API更新tb_basics基础信息-----------------------------------------'''
	def update_stock_basics(self):

		df = ts.get_stock_basics()
		df['code'] = df.index
		df.to_sql('tb_basics', con=self._db, if_exists='replace', index=False)
		pass

	
	def update_current_price(self):

		current_date = local_time()

		hold_date = time.strftime('%Y-%m-%d', current_date.timetuple())

		print(hold_date)
		'''
		resultProxy = self._db.execute(
				text('select code from tb_basics'))
		result_list = resultProxy.fetchall()

		for code in result_list:

			print(code[0])

			#历史交易数据
			df = ts.get_k_data(code[0], start='2018-05-01', end='2018-05-08')
			print(df)
			df.to_sql('tb_history_price', con=self._db, if_exists='append', index=False)

		'''
		df = ts.get_today_all()
		df.loc[:, 'date'] = hold_date
		df = df.rename(index=str, columns={"trade": "close"})
		df.to_sql('tb_history_price_1', con=self._db, if_exists='append', index=False)

		pass
	

	# 所有交易日期
	def update_history_price(self, code):
		
		#历史交易数据
		df = ts.get_k_data(code)
		df.to_sql('tb_history_price', con=self._db, if_exists='replace', index=False)

		pass

	# 指定交易日期
	def update_history_price_by_date(self, code, start_date, end_date):

		#历史交易数据
		df = ts.get_k_data(code, index = False, start = start_date, end = end_date)
		df.to_sql('tb_history_price', con=self._db, if_exists='append', index=False)
		pass






