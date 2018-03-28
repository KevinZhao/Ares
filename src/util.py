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
import urllib.request
import urllib.error
import random
import socket

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

    #print(log_str)

    fd = open('Ares.log', 'r')
    fd.flush()
    log = fd.read()
    fd.close()

    fd = open('Ares.log', 'w')

    fd.write(log + timestamp() + ' ' + log_str + '\n')
    fd.flush()
    fd.close()

def clear_log():
    fd = open('Ares.log', 'w')

    fd.write('')
    fd.flush()
    fd.close()

def save_cookie(cookie):

    fd = open('cookie', 'w')

    fd.write(cookie)
    fd.flush()
    fd.close()

def read_cookie():

    fd = open('cookie', 'r')

    cookie = fd.read()
    fd.close()

    return cookie

def url_request_with_retry(url, parameters, retry_count, sleep_count):

    socket.setdefaulttimeout(15)
    
    attempts = 0
    success = False
    result = ''

    data = urllib.parse.urlencode(parameters) 
    data = data.encode('ascii')

    while attempts <= retry_count and not success:

        ua_list = [
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv2.0.1) Gecko/20100101 Firefox/4.0.1",
            "Mozilla/5.0 (Windows NT 6.1; rv2.0.1) Gecko/20100101 Firefox/4.0.1",
            "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
            "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36"
            ]
        user_agent = random.choice(ua_list)

        connection = "keep-alive"
        host = "sc.hkexnews.hk"
        cookie = read_cookie()

        request = None
        if len(parameters) == 0:
            request = urllib.request.Request(url, data, method = 'GET')
        else:
            request = urllib.request.Request(url, data, method = 'POST')
        request.add_header('User-Agent', user_agent)
        
        if cookie != "":
            request.add_header('Cookie', cookie)

        request.add_header('Connection', connection)
        request.add_header('Host', host)

        try:
            #log(url)
            response = urllib.request.urlopen(request, data)
            result = response.read().decode('utf-8')
            
            cookie = ""

            for header in response.getheaders():
                if header[0] == 'Set-Cookie':
                    cookie = cookie + header[1]+ ';'

            response.close()

            save_cookie(cookie)
            success = True

            time.sleep(sleep_count)

        except urllib.error.HTTPError as e:
            log('urllib.error.HTTPError')
            log(str(e.code))
            log(e.reason)

            cookie = ''
            save_cookie(cookie)

            attempts += 1

            continue
        except ConnectionResetError as e:
            log('ConnectionResetError')
            attempts += 1

        except urllib.error.URLError as e:
            #TimeoutError, [Errno 110] Connection timed out
            #log(e.reason)
            #print(e)
            log('urllib.error.URLError')
            attempts += 1

        except socket.timeout:
            log('socket.timeout')
            attempts += 1

        except httplib.BadStatusLine as e:
            log('http.client.BadStatusLine')
            log(str(e))
            attempts += 1

        except Exception as e:

            log(str(e))
            attempts += 1

        if attempts == retry_count + 1:
            log('[Fatal Error]' + url)

    return result

def timestamp():
    current_time = local_time()
    timestamp = time.strftime('%Y%m%d_%H%M%S', current_time.timetuple())

    return timestamp


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





        