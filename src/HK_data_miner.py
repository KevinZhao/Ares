# -*- coding:utf-8 -*- 
from bs4 import BeautifulSoup
from src.util import *
from src.repeatTimer import *
from functools import partial

import numpy as np
import pandas as pd
import urllib.request
import time
import datetime
import re
import os
import os.path
import shutil

class HK_data_miner():
	_name = 'HK_data_miner'
	_viewstate = ''
	_viewgenerator = ''
	_eventvalidation = ''
	market_type_list = ['sz', 'sh']

	_detail_downloaded = False
	_detail_downloaded_count = 0

	def get_current_data(self):

		print('get_current_data called')

    	#获取当日数据
		result = ''

		for market_type in self.market_type_list:

		    url = "http://sc.hkexnews.hk/TuniS/www.hkexnews.hk/sdw/search/mutualmarket_c.aspx?t=" + market_type
		    response = urllib.request.urlopen(url)
		    result = response.read().decode('utf-8')
		    
		    current_time = local_time()

		    yesterday = (current_time - datetime.timedelta(days = 1))
		    
		    file_date = time.strftime('%Y%m%d', yesterday.timetuple())
		    filename = "data/"+ market_type + file_date + ".csv"
		    hold_date = time.strftime('%d%m%Y', yesterday.timetuple())

		    self.save_to_csv(result, filename, hold_date)
		    self._viewstate, self._viewgenerator, self._eventvalidation = self.store_view_para(result)

		    pass

	def get_history_data(self):

		if self._viewstate == '':
			self.get_current_data()

		#这个写法不太好
		for i in range(1, 170):

			for market_type in self.market_type_list:

				url = "http://sc.hkexnews.hk/TuniS/www.hkexnews.hk/sdw/search/mutualmarket_c.aspx?t=" + market_type

				date = (datetime.datetime.now() - datetime.timedelta(days = i))

				history_y = time.strftime('%Y', date.timetuple());
				history_m = time.strftime('%m', date.timetuple());
				history_d = time.strftime('%d', date.timetuple());
				hold_date = time.strftime('%d%m%Y', date.timetuple())
				week_day = time.strftime("%w", date.timetuple())

				#周日，周一不取数据
				if (int(week_day) > 0) and (int(week_day)) < 6:
					#发送post 请求
					values = {
					    '__VIEWSTATE': self._viewstate,
					    '__VIEWSTATEGENERATOR': self._viewgenerator,
					    '__EVENTVALIDATION':self._eventvalidation,
					    'today':'20171119',
					    'ddlShareholdingDay':history_d,
					    'ddlShareholdingMonth':history_m,
					    'ddlShareholdingYear':history_y,
					    'btnSearch.x' :'37',
					    'btnSearch.y' :'12'
					}
					data = urllib.parse.urlencode(values) 
					data = data.encode('ascii')

					request = urllib.request.Request(url, data, method='POST')
					response = urllib.request.urlopen(request, data)
					result = response.read().decode('utf-8')

					history_date = time.strftime('%Y%m%d', date.timetuple())
					filename = "data/"+ market_type + history_date + ".csv"

					self.save_to_csv(result, filename, hold_date)
					self._viewstate, self._viewgenerator, self._eventvalidation = self.store_view_para(result)

	def save_to_csv(self, html, filename, hold_date):
    
	    df = pd.DataFrame(columns = ['No', 'Name', 'Volume', 'Percent'])
	   
	    soup = BeautifulSoup(html, "html5lib")
	    
	    holding = soup.find('div', {'style':'margin: 20px 0 0 10px; font-weight: bold; text-decoration: underline;'})
	    holding_date = holding.string
	    holding_date = re.sub("\D", "", holding_date) 

	    if holding_date != hold_date:
	        return
	    
	    #寻找Table
	    tr_list = soup.findAll('tr', {"class":'row0'})
	    #处理数据
	    for tr in tr_list:
	        record = []
	        for child in tr.children:
	            str = child.string.replace('\n','')
	            str = str.replace(' ','')
	            if str != '':
	                record.append(str)
	        df.loc[df.shape[0]+1] = record
	        
	    tr_list = soup.findAll('tr', {"class":'row1'})
	    #处理数据
	    for tr in tr_list:
	        record = []
	        for child in tr.children:
	            str = child.string.replace('\n','')
	            str = str.replace(' ','')
	            if str != '':
	                record.append(str)
	        df.loc[df.shape[0]+1] = record
	        
	    df = df.set_index('No')
	    df.to_csv(filename, encoding='gbk')
	    print('save to ', filename)
	    
	    return

	def store_view_para(self, html):
    
	    soup = BeautifulSoup(html, "html5lib")
	    
	    #提取页面参数
	    content = soup.find('input', {"id":"__VIEWSTATE"}, 'value')
	    _viewstate = content.attrs['value']
	    content = soup.find('input', {"id":"__VIEWSTATEGENERATOR"}, 'value')
	    _viewgenerator = content.attrs['value']
	    content = soup.find('input', {"id":"__EVENTVALIDATION"}, 'value')
	    _eventvalidation = content.attrs['value']
	    
	    return _viewstate, _viewgenerator, _eventvalidation

	def write_to_mysqldb(self):

		print('write_to_mysqldb called')

		data_dir = 'data/'
		archive_dir = 'archive/'

		for parent,dirnames,filenames in os.walk(data_dir):    #三个参数：分别返回1.父目录 2.所有文件夹名字（不含路径） 3.所有文件名字
			for filename in filenames:

				#.DStore file remove
				if filename[0] == '.':
				    print('.DStore file error skipped')
				    continue;
				
				market_type = filename[0:2]
				date = filename[2:10]

				fullpath = data_dir + filename
				archive_path =  archive_dir + filename
				
				#1. read csv
				df_daily = pd.read_csv(fullpath, encoding='gbk')

				#2. volume optimize
				df_daily['Volume'] = df_daily['Volume'].astype(str).str.replace(',','')
				df_daily['Volume'] = pd.to_numeric(df_daily['Volume'], errors = 'coerce')

				#3. Percent optimize
				df_daily['Percent'] = df_daily['Percent'].astype(str).str.replace('%','')
				df_daily['Percent'] = pd.to_numeric(df_daily['Percent'], errors = 'coerce')

				df_daily['Date'] = date

				# wirte to database
				db = Jiatou_DB()._db
				
				df_daily.to_sql('tb_daily', con=db, if_exists='append', index=False)

				#copy file from data to archive
				open(archive_path, "wb").write(open(fullpath, "rb").read())
				os.remove(fullpath)

				self._detail_downloaded_count = self._detail_downloaded_count + 1

				print(fullpath, "write to database")
				pass

		if self._detail_downloaded_count == 2:
			self._detail_downloaded = True



