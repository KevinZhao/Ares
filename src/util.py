# -*- coding:utf-8 -*- 

import numpy as np
import matplotlib.pyplot as plt  
import pandas as pd
import os
import os.path
import time, datetime
from sqlalchemy import create_engine
from sqlalchemy import text
import tushare as ts
import csv
import json

''' ---------返回当地时间-----------------------------------------'''
def local_time():
    
    dt = datetime.datetime.utcnow()
    dt = dt.replace(tzinfo=datetime.timezone.utc)
    tzutc_8 = datetime.timezone(datetime.timedelta(hours=8))
    local_dt = dt.astimezone(tzutc_8)
    
    return(local_dt)

''' ---------港股通code转换成A股Code-----------------------------------------'''
def no_to_code(no):
    if no[0] == '9':
        
        no = no[1:5]
        code = '60'+ no
        
    if no[0] == '7':
        if no[1] != '7':
            no = no[1:5]
            code = '00'+ no
        else:
            no = no[2:5]
            code = '300'+ no
    
    return code

''' ---------代码转换成名称-----------------------------------------'''
def code_to_name(code):

    db = Jiatou_DB()._db

    resultProxy=db.execute(
        text('select name from tb_basics where code = :code '), {'code':code})
    name = resultProxy.fetchall()
    
    return name[0][0]

def name_to_code(name):

    db = Jiatou_DB()._db

    resultProxy=db.execute(
        text('select code from tb_basics where name = :name '), {'name':name})
    code = resultProxy.fetchone()[0]

    return code

def log(log_str):

    fd = open('Ares.log', 'r')
    fd.flush()
    log = fd.read()
    fd.close()

    fd = open('Ares.log', 'w')

    fd.write(log + log_str + '\n')
    fd.flush()
    fd.close()


class Singleton(type):
        """Singleton Metaclass"""
        def __init__(cls, name, bases, dic):
                super(Singleton, cls).__init__(name, bases, dic)
                cls.instance = None

        def __call__(cls, *args, **kwargs):
                if cls.instance is None:
                        cls.instance = super(Singleton, cls).__call__(*args, **kwargs)
                return cls.instance

class Jiatou_DB(object):  
        __metaclass__ = Singleton  

        _db = None
        
        def __init__(self): 
            with open('src/Jiatou.cnf') as json_data:
                cnf = json.load(json_data)                
                self._db = create_engine(cnf['db'])



        