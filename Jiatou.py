# -*- coding:utf-8 -*- 
import os
import time
from src.HK_data_miner import *
from src.util import *
from src.Tushare_adapter import *
from src.HK_data_processing import *
from apscheduler.schedulers.background import BackgroundScheduler

def get_HK_detail():

	#download
	miner.get_current_data()
	miner.write_to_mysqldb()

	#update
	data_proc = HK_data_processing()
	data_proc.update_HK_detail()

	pass

def get_HK_amount_flow():
	print('get_HK_amount_flow not implemented')

def get_HK_amount_top():
	print('get_HK_amount_flow not implemented')

def jiatou():
	miner = HK_data_miner()
	sched = BackgroundScheduler()

	#周二至周六对应 3点执行 获取每日持仓情况, 执行一次
	sched.add_job(
		get_HK_detail, 'cron', 
		hour='03', 
		minute='00', 
		day_of_week ='1-6', 
		timezone = 'Asia/Shanghai')

	'''	
	#周一至周五对应 日9点半至11点半，13点至15点，获取资金流入流出，每秒执行一次
	sched.add_job(
		get_HK_amount_flow, 'cron', 
		hour='09-15', 
		minute='0-59', 
		second = '0-59', 
		timezone = 'Asia/Shanghai')

	
	#周一至周五对应 日5点40分，获取资金流入流出，执行一次
	sched.add_job(
		get_HK_amount_top, 'cron', 
		hour='5', 
		minute='40-45', 
		timezone = 'Asia/Shanghai')
	'''

	sched.start()

def log(log_str):

	fd = open('/tmp/demone.log', 'w')
	
	while True:
		fd.write(log_str + '\n')
		fd.flush()
		time.sleep(2)
	fd.close()

def createDaemon():    

	# create - fork 1
	try:
		if os.fork() > 0: 
			os._exit(0)
		
	except OSError as error:
		
		print('fork #1 failed: %d (%s)' % (error.errno, error.strerror))
		os._exit(1)

	# it separates the son from the father
	os.chdir('/')
	os.setsid()
	os.umask(0)

	# create - fork 2
	try:
		pid = os.fork()
		if pid > 0:
			print('Daemon PID %d' % pid)
			os._exit(0)
	except OSError as error:
		print('fork #2 failed: %d (%s)' % (error.errno, error.strerror))
		os._exit(1)

	jiatou()

if __name__ == '__main__':

	createDaemon()