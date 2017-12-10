# -*- coding:utf-8 -*- 
from src.HK_data_miner import *
from src.repeatTimer import *
from src.Tushare_adapter import *
from src.HK_data_processing import *

def get_HK_detail():

	if miner._detail_downloaded == False:
    
		miner.get_current_data()
		miner.write_to_mysqldb()
		update_HK_detail()

		print(miner._detail_downloaded)

def store_history_data(no):
    #历史交易数据
    code = no_to_code(no)
    
    ts_adapter = Tushare_adapter()
    ts_adapter.update_history_price(code)


def update_HK_detail():

	# read from database
	db = Jiatou_DB()._db

	#港股通代码转换
	resultProxy=db.execute(text('select distinct no from tb_daily'))
	no_result = resultProxy.fetchall()
	result_array= []

	#循环遍历求差值
	for no in no_result:
	    
	    store_history_data(no[0])

	    print('processing code:', no[0])
	    data_processor = HK_data_processing()
	    data_processor.update_amount(no[0])
	    data_processor.update_amount_diff(no[0])


miner = HK_data_miner()

r = RepeatTimer(60.0, get_HK_detail)
r.start()

#获取时间：
#当日是周二至周六时，获取前一日详细数据


#当日是周一至周五时，获取陆股通实时资金数据，当日晚5点40分后，获取十大成交数据

