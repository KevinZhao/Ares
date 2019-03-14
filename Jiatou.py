# -*- coding:utf-8 -*- 
import os
import time
from src.HK_data_miner import *
from src.util import *
from src.Tushare_adapter import *
from src.HK_data_processing import *
from src.Lixinger_adapter import *
from apscheduler.schedulers.background import BackgroundScheduler

def get_holding_data():

	miner = HK_data_miner()

	try:
		#download
		miner.get_holding_data()
		#miner.get_holding_data_history('20190129', '20190313')
		miner.write_holding_data_mysqldb()
	
	except Exception as e:
		log(str(e))

	pass

def get_detail_data():

	miner = HK_data_miner()

	try:
		#download
		miner.get_detail_data()		
	
	except Exception as e:
		log(str(e))

	pass


def update_basic_data():

	Lxr_adapter = Lixinger_adapter()

	try:
		#update volume and amount information
		Lxr_adapter.update_fundamental_info_history()
		
	except Exception as e:
		log(str(e))

	pass


def update_holding_data():

	data_proc = HK_data_processing()

	try:
		#update volume and amount information
		data_proc.update_holding_detail()
		data_proc.update_trend_history()
	
	except Exception as e:
		log(str(e))

	pass

def update_basics():

	ts_adatper = Tushare_adapter()

	try:
		ts_adatper.update_stock_basics()

	except Exception as e:
		log(str(e))

	pass


def get_HK_amount_flow():
	log('get_HK_amount_flow not implemented')

def get_HK_amount_top():
	log('get_HK_amount_top not implemented')

def jiatou():
	
	clear_log()
	save_cookie('')
	#scheduleTask()
	update_holding_data()
	#get_holding_data()
	
def scheduleTask():

	sched = BackgroundScheduler()

	#周一至周五对应 17点执行 更新数基本数据，每日执行一次
	sched.add_job(
		update_basics, 'cron', 
		hour='17', 
		minute='00', 
		day_of_week ='0-5', 
		timezone = 'Asia/Shanghai')

	#周二至周六对应 5点执行 获取每日持仓数量情况，每日执行一次
	sched.add_job(
		get_holding_data, 'cron', 
		hour='05', 
		minute='00', 
		day_of_week ='1-6', 
		timezone = 'Asia/Shanghai')

	#周二至周六对应 5点15分执行 获取每日持仓数量明细情况，每日执行一次
	sched.add_job(
		update_holding_data, 'cron', 
		hour='5', 
		minute='15', 
		day_of_week ='1-6', 
		timezone = 'Asia/Shanghai')

	#LXR
	'''
	sched.add_job(
		update_basic_data, 'cron', 
		hour='23', 
		minute='35', 
		timezone = 'Asia/Shanghai')
	'''
	'''
	#周一至周五对应 日9点半至11点半，13点至15点，获取资金流入流出，每秒执行一次
	sched.add_job(
		get_HK_amount_flow, 'cron', 
		hour='09-15', 
		minute='0-59', 
		second = '0-59', 
		timezone = 'Asia/Shanghai')

	#周一至周五对应 日5点40分，获取资金流入流出，，每秒执行一次
	sched.add_job(
		get_HK_amount_top, 'cron', 
		hour='5', 
		minute='40-45', 
		second = '0-59', 
		timezone = 'Asia/Shanghai')
	'''
	sched.start()

	while True:
 		time.sleep(1)


def createDaemon():    

	# create - fork 1
	try:
		if os.fork() > 0: 
			os._exit(0)
		
	except OSError as error:
		
		log('fork #1 failed: %d (%s)' % (error.errno, error.strerror))
		os._exit(1)

	# it separates the son from the father
	os.chdir('/root/Ares/') #todo: change to /
	os.setsid()
	os.umask(0)

	# create - fork 2
	try:
		pid = os.fork()
		if pid > 0:
			log('Daemon PID %d' % pid)
			os._exit(0)
	except OSError as error:
		log('fork #2 failed: %d (%s)' % (error.errno, error.strerror))
		os._exit(1)

	jiatou()

if __name__ == '__main__':

	createDaemon()
	#get_HK_detail_history()