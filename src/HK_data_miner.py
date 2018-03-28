# -*- coding:utf-8 -*- 
from bs4 import BeautifulSoup
from src.util import *
from functools import partial

import numpy as np
import pandas as pd
import time
import datetime
import re
import os
import os.path
import shutil
import json

class HK_data_miner():
	_name = 'HK_data_miner'
	_viewstate = ''
	_viewgenerator = ''
	_eventvalidation = ''
	_db = Jiatou_DB()._db
	market_type_list = ['sz', 'sh']
	_sleep_timer = 12
	_retry_count = 5

	#---For Holding Data-----#

	def get_holding_data(self, holding_date = None):

		log('get_holding_data called %s' % holding_date)

		#formate date 
		if holding_date != None :
			date_struct = time.strptime(holding_date, "%Y%m%d")
			date = datetime.datetime(* date_struct[:6])
		else:
			current_time = local_time()
			date = (current_time - datetime.timedelta(days = 1))

		history_y = time.strftime('%Y', date.timetuple());
		history_m = time.strftime('%m', date.timetuple());
		history_d = time.strftime('%d', date.timetuple());
		hold_date = time.strftime('%d%m%Y', date.timetuple())
		week_day = time.strftime("%w", date.timetuple())

		#周日，周一不取数据
		if (int(week_day) <= 0) or (int(week_day)) >= 6:
			return

		json_file = open("day_list.json", encoding='utf-8')
		day_list = json.load(json_file)

		#法定节假日不取数据
		for day in day_list['day_list']:
			if day == holding_date:
				log('get_holding_data return as holding_date in day_list %s' % holding_date)
				return

    	#获取页面参数，防止session过期
		result = ''

		url = "http://sc.hkexnews.hk/TuniS/www.hkexnews.hk/sdw/search/mutualmarket_c.aspx?t=sh"
		parameters = []
		result = url_request_with_retry(url, parameters, self._retry_count, self._sleep_timer)

		if result != '':
			self._viewstate, self._viewgenerator, self._eventvalidation = self.store_view_para(result)
		else:
			self.get_holding_data(holding_date)

		for market_type in self.market_type_list:

			url = "http://sc.hkexnews.hk/TuniS/www.hkexnews.hk/sdw/search/mutualmarket_c.aspx?t=" + market_type

			parameters = {
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

			result = url_request_with_retry(url, parameters, self._retry_count, self._sleep_timer)

			if result != '':

				history_date = time.strftime('%Y%m%d', date.timetuple())
				filename = "data/"+ market_type + history_date + ".csv"

				self.save_holding_data_csv(result, filename, hold_date)
				self._viewstate, self._viewgenerator, self._eventvalidation = self.store_view_para(result)

			else:
				print('failed and try again')
				self.get_holding_data(holding_date)

			pass

	def get_holding_data_history(self, start_date, end_date):

		log('get_holding_data_history called')

		start_date_struct = time.strptime(start_date, "%Y%m%d")
		start_date = datetime.datetime(* start_date_struct[:6])
		
		end_date_struct = time.strptime(end_date, "%Y%m%d")
		end_date = datetime.datetime(* end_date_struct[:6])
		
		for i in range((end_date - start_date).days + 1):  
			date = start_date + datetime.timedelta(days=i)
			date = date.strftime('%Y%m%d')

			self.get_holding_data(date)

			time.sleep(1)

	def save_holding_data_csv(self, html, filename, hold_date):
	    #log(html)
    
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

	    #log('save to ', filename)
	    return

	def write_holding_data_mysqldb(self):

		log('write_holding_data_mysqldb called')

		data_dir = 'data/'
		archive_dir = 'archive/'

		for parent,dirnames,filenames in os.walk(data_dir):    #三个参数：分别返回1.父目录 2.所有文件夹名字（不含路径） 3.所有文件名字
			for filename in filenames:

				#.DStore file remove
				if filename[0] == '.':
				    log('.DStore file error skipped')
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

				log("%s write to database" % fullpath)
				pass

	#---For Detail Data-----#

	def get_detail_data(self, detail_date = None):

		log('get_data_detail called %s'  % detail_date)

		#formate date 
		if detail_date != None :
			date_struct = time.strptime(detail_date, "%Y%m%d")
			date = datetime.datetime(* date_struct[:6])
		else:
			current_time = local_time()
			date = (current_time - datetime.timedelta(days = 1))

		date_Ymd = time.strftime('%Y%m%d', date.timetuple());
		result = ''

		resultProxy = self._db.execute(text('select distinct no from tb_daily where date =:date'), {'date':date_Ymd})
		no_list = resultProxy.fetchall()

		for no in no_list:
			log(no[0])

			#check if data already in the database
			resultProxy = self._db.execute(
				text('select no from tb_detail where no = :no and date = :date'), {'no':no[0], 'date':date_Ymd})
			result_list = resultProxy.fetchall()

			if len(result_list) == 0:

				url = "http://sc.hkexnews.hk/TuniS/www.hkexnews.hk/sdw/search/searchsdw_c.aspx"
				parameters = []
				result = url_request_with_retry(url, parameters, self._retry_count, self._sleep_timer)

				self._viewstate, self._viewgenerator, self._eventvalidation = self.store_view_para(result)

				ddlShareholdingYear = time.strftime('%Y', date.timetuple());
				ddlShareholdingMonth = time.strftime('%m', date.timetuple());
				ddlShareholdingDay = time.strftime('%d', date.timetuple());

				parameters = {
				    '__VIEWSTATE': self._viewstate,
				    '__VIEWSTATEGENERATOR': self._viewgenerator,
				    '__EVENTVALIDATION':self._eventvalidation,
				    'ddlShareholdingDay':ddlShareholdingDay,
				    'ddlShareholdingMonth':ddlShareholdingMonth,
				    'ddlShareholdingYear':ddlShareholdingYear,
				    'txtStockCode':no[0],
				    'btnSearch.x' :'17',
				    'btnSearch.y' :'11'
				}

				result = url_request_with_retry(url, parameters, self._retry_count, self._sleep_timer)

				if result != '':
					filename = "detail_data/"+ no[0] + '_'+ date_Ymd + ".csv"
					self.save_detail_data_csv(no[0], result, filename, date)

		pass

	def get_detail_data_history(self, start_date, end_date):

		log('get_detail_data_history called')

		start_date_struct = time.strptime(start_date, "%Y%m%d")
		start_date = datetime.datetime(* start_date_struct[:6])
		
		end_date_struct = time.strptime(end_date, "%Y%m%d")
		end_date = datetime.datetime(* end_date_struct[:6])
		
		for i in range((end_date - start_date).days + 1):  
			date = start_date + datetime.timedelta(days=i)
			date = date.strftime('%Y%m%d')

			self.get_detail_data(date)

	def save_detail_data_csv(self, no, html, filename, hold_date):
    
		df = pd.DataFrame(columns = ['No', 'Holder', 'Name', 'Address','Volume', 'Percent', 'Date'])
		hold_date_dmY = time.strftime('%d%m%Y', hold_date.timetuple())

		soup = BeautifulSoup(html, "html5lib")

		#find td bgcolor="#F5F5F5"
		td = soup.find('td', {"bgcolor":'#F5F5F5'})

		try:
			td_child = td.findAll('td', {"class":'arial12black', "nowrap":'nowrap'}) 
		except AttributeError as e:
			log(html)
			return
			

		holding_date = td_child[1].string
		holding_date = re.sub("\D", "", holding_date)

		if holding_date != hold_date_dmY:
		    return

		hold_date = time.strftime('%Y%m%d', hold_date.timetuple())
		
	    #find data table
		tr_list = soup.findAll('tr', {"class":'row0'})
		tr_list_1 = soup.findAll('tr', {"class":'row1'})

		tr_list.extend(tr_list_1)

	    #处理数据
		for tr in tr_list:

			record = []
			record.append(no)

			for td in tr.children:	
				if td.name == 'td':

					for child in td.children:

						result = child.string
						if result != None:
							#字符串处理，掐头去尾, 去掉换行
							result = result.replace('\n','')

							for i in range(len(result)):
								if result[0] == ' ':
									result = result[1:]
								else:
									break;

							for i in range(len(result)):
								if result[len(result) - 1] == ' ':
									result = result[0:len(result)-2]
								else:
									break;
							
							if result != '':
								record.append(result)

			record.append(hold_date)

			#香港中央结算有限公司 Holder is null
			if len(record) != 7:
				record.insert(1, '')

			df.loc[df.shape[0]+1] = record
	        
		df = df.set_index('No')
		df.to_csv(filename, encoding='gbk')
	    
		return

	def write_detail_data_mysqldb(self):

		log('write_to_mysqldb_detail called')

		data_dir = 'detail_data/'
		archive_dir = 'detail_archive/'

		for parent,dirnames,filenames in os.walk(data_dir):    #三个参数：分别返回1.父目录 2.所有文件夹名字（不含路径） 3.所有文件名字
			for filename in filenames:

				#.DStore file remove
				if filename[0] == '.':
				    log('.DStore file error skipped')
				    continue;
				
				code = filename[0:5]
				date = filename[6:14]

				fullpath = data_dir + filename
				archive_path =  archive_dir + filename
				
				#1. read csv
				df_detail = pd.read_csv(fullpath, encoding='gbk')

				#2. volume optimize
				df_detail['Volume'] = df_detail['Volume'].astype(str).str.replace(',','')
				df_detail['Volume'] = pd.to_numeric(df_detail['Volume'], errors = 'coerce')

				#3. Percent optimize
				df_detail['Percent'] = df_detail['Percent'].astype(str).str.replace('%','')
				df_detail['Percent'] = pd.to_numeric(df_detail['Percent'], errors = 'coerce')

				# wirte to database
				db = Jiatou_DB()._db
				
				df_detail.to_sql('tb_detail', con=db, if_exists='append', index=False)

				#copy file from data to archive
				open(archive_path, "wb").write(open(fullpath, "rb").read())
				os.remove(fullpath)

				pass

	#---For amount flow-----#
	def get_amount_flow(self):

		log('get_amount_flow called')


		url = "http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/script/data_NBSZ_Turnover_chi.js"
		parameters = {
			    '_': '1521991397470'
			}

		result = url_request_with_retry(url, parameters, self._retry_count, self._sleep_timer)

		print(result)


	#---Miner Util-----#
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







