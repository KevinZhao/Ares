# -*- coding:utf-8 -*- 
from src.HK_data_miner import *
from src.util import *
from src.Tushare_adapter import *
from src.HK_data_processing import *
import time
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

miner = HK_data_miner()
sched = BackgroundScheduler()

get_HK_detail()

#周二至周六对应 1点执行 获取每日持仓情况, 执行一次
sched.add_job(
	get_HK_detail, 'cron', 
	hour='1', 
	day_of_week ='2-6', 
	timezone = 'Asia/Shanghai')

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

sched.start()


