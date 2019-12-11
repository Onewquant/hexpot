###########################################
## System, File, Data Structure 관련 import
###########################################

import os
import glob
import shutil
from itertools import chain

###########################################
##         Math, 자료형 관련 import
###########################################

import json
import math
import random

###########################################
##         Data 받기
###########################################

import requests
from io import BytesIO

###########################################
## mssql 관련 import
###########################################

import pyodbc
import sqlalchemy

###########################################
##           자동화 관련
###########################################

import pythoncom
import win32com.client
import pywinauto
from pywinauto import application, timings
import pyautogui

###########################################
## PyQt 관련 import
###########################################

from PyQt5.QtWidgets import *
from PyQt5 import uic
from PyQt5.QtGui import *
from PyQt5.QtCore import *
from PyQt5.QAxContainer import *

###########################################
##         데이터분석 툴
###########################################

import pandas as pd
from pandas.io.json import json_normalize
import numpy as np

###########################################
##         Process, Thread
###########################################

from matplotlib import font_manager, rc, pyplot as plt
import seaborn as sns

###########################################
##         Process, Thread
###########################################

try:
    import thread
except ImportError:
    import _thread as thread

###########################################
##          날짜관련 import
###########################################

import time
from datetime import datetime, timedelta
import calendar
from dateutil.relativedelta import relativedelta
import pytz

###########################################
## Multiprocessing 관련
###########################################

from multiprocessing import Pool, Queue, Process, Pipe, freeze_support

###########################################
##              Asyncio 관련
###########################################

import concurrent.futures
import asyncio

###########################################
##          websocket 관련
###########################################

import websocket

###########################################
##              Zero MQ
###########################################

import zmq

###########################################
##            Protocol Buffers
###########################################

from hexpot.hexpot_pb import cpformat_pb2, cdformat_pb2, kstdformat_pb2, kstpohlc_pb2
from google.protobuf.json_format import MessageToJson
import stream

###########################################################################
##                          General Tools
###########################################################################


## 프로그램 작동 시간 측정 데코레이터
"""
opening_time = time.time()
closing_time = time.time()
print(closing_time-opening_time)
"""
# def runtime(f):
#     def wrapper(*args, **kwargs):
#         import time
#         start = time.time()
#         f
#         end = time.time()
#         print(end - start)
#         return f()
#     return wrapper

def timestamp_into_kst_time(ts):
    KST = pytz.timezone('Asia/Seoul')
    try:
        dt = datetime.utcfromtimestamp(ts)
        k_dt = pytz.utc.localize(dt).astimezone(KST)
        return k_dt
    except:
        return np.nan


## Date list generator

def generate_day_list(start_date,end_date=None):

    start_date = datetime.strptime(start_date,'%Y-%m-%d')

    if end_date == None :
        end_date = datetime.today()
    else :
        end_date = datetime.strptime(end_date,'%Y-%m-%d')

    delta = end_date - start_date
    date_list = []

    for i in range(delta.days + 1):
        d = start_date + timedelta(days=i)
        date_list = date_list + [d.strftime("%Y-%m-%d")]

    return date_list

def generate_day_index_df(start_date,end_date=None):

    start_date = datetime.strptime(start_date,'%Y-%m-%d')

    if end_date == None :
        end_date = datetime.today()
    else :
        end_date = datetime.strptime(end_date,'%Y-%m-%d')

    delta = end_date - start_date
    date_list = []

    for i in range(delta.days + 1):
        d = start_date + timedelta(days=i)
        date_list = date_list + [d.strftime("%Y-%m-%d")]

    df = pd.DataFrame(index=date_list)
    df.index = pd.to_datetime(df.index)

    return df


def check_dir(dir_path):
    if os.path.exists(dir_path):
        pass
    else:
        os.mkdir(dir_path)

def mk_new_dir(dir_path):
    if os.path.exists(dir_path):
        shutil.rmtree(path=dir_path)
    os.mkdir(dir_path)


###########################################################################
##                          Coin Data File 다루기
###########################################################################


def load_single_market_coin_event_data_generator(market, coin, start_dt_str, end_dt_str, db_path, pb_cls=cdformat_pb2.coinevent):

    start_date = start_dt_str.split('T')[0]
    end_date = end_dt_str.split('T')[0]

    start_ts_str = str(int(datetime.strptime(start_dt_str,'%Y-%m-%dT%H:%M:%S').timestamp()*1000))
    end_ts_str = str(int(datetime.strptime(end_dt_str,'%Y-%m-%dT%H:%M:%S').timestamp()*1000))

    to_do_candidate_list = generate_day_list(start_date=start_date,end_date=end_date)

    file_name = '{}_{}_****-**-**.gz'.format(market,coin)
    file_path = '{}\\{}\\{}\\{}'.format(db_path,market,coin,file_name)
    file_list = glob.glob(file_path)
    to_do_file_path_list = []
    to_do_date_list = []

    for fp in file_list:
        file_date = fp.split('{}_{}_'.format(market,coin))[-1][:-3]
        if file_date in to_do_candidate_list:
            to_do_date_list.append(file_date)
            to_do_file_path_list.append(fp)

    to_do_file_path_list.sort(reverse=False)

    for fp_inx in range(len(to_do_file_path_list)):
        if fp_inx==0:
            fp = to_do_file_path_list[fp_inx]
            t = stream.parse(ifp=fp, pb_cls=pb_cls)
            continue
        fp = to_do_file_path_list[fp_inx]
        f = stream.parse(ifp=fp, pb_cls=pb_cls)
        t = chain(t,f)

    def filtered_gen_func():
        for x in t:
            if (x.tms < start_ts_str)|(x.tms >= end_ts_str):
                continue
            yield x

    return filtered_gen_func()


def load_single_market_coin_event_data_bundle(market, coin, start_dt_str, end_dt_str, db_path, pb_cls=cdformat_pb2.coinevent):

    start_date = start_dt_str.split('T')[0]
    end_date = end_dt_str.split('T')[0]

    start_ts_str = str(int(datetime.strptime(start_dt_str,'%Y-%m-%dT%H:%M:%S').timestamp()*1000))
    end_ts_str = str(int(datetime.strptime(end_dt_str,'%Y-%m-%dT%H:%M:%S').timestamp()*1000))

    to_do_candidate_list = generate_day_list(start_date=start_date,end_date=end_date)

    file_name = '{}_{}_****-**-**.gz'.format(market,coin)
    file_path = '{}\\{}\\{}\\{}'.format(db_path,market,coin,file_name)
    file_list = glob.glob(file_path)
    to_do_file_path_list = []
    to_do_date_list = []

    for fp in file_list:
        file_date = fp.split('{}_{}_'.format(market,coin))[-1][:-3]
        if file_date in to_do_candidate_list:
            to_do_date_list.append(file_date)
            to_do_file_path_list.append(fp)

    to_do_file_path_list.sort(reverse=False)

    for fp_inx in range(len(to_do_file_path_list)):
        if fp_inx==0:
            fp = to_do_file_path_list[fp_inx]
            t = stream.parse(ifp=fp, pb_cls=pb_cls)
            continue
        fp = to_do_file_path_list[fp_inx]
        f = stream.parse(ifp=fp, pb_cls=pb_cls)
        t = chain(t,f)

    return t


def load_combined_coin_event_data_generator(market_coin_comb, start_dt_str, end_dt_str, db_path, pb_cls=cdformat_pb2.coinevent):

    start_ts_str = str(int(datetime.strptime(start_dt_str,'%Y-%m-%dT%H:%M:%S').timestamp()*1000))
    end_ts_str = str(int(datetime.strptime(end_dt_str,'%Y-%m-%dT%H:%M:%S').timestamp()*1000))

    market_coin_bundle_dict = dict()
    bn = 0
    for m in market_coin_comb.keys():
        for c in market_coin_comb[m]:
            try:
                mcfg = load_single_market_coin_event_data_bundle(market=m,coin=c,start_dt_str=start_dt_str,end_dt_str=end_dt_str,db_path=db_path)
                market_coin_bundle_dict.update({bn:mcfg})
                bn+=1
            except:
                print('{} / {} / {} / {} /데이터가 존재하지 않습니다'.format(m,c,start_dt_str,end_dt_str))
                continue

    if len(market_coin_bundle_dict)==0:
        print('데이터가 존재하지 않습니다'.format(m, c, start_dt_str, end_dt_str))

        class finish_msg():
            def __init__(self):
                self.type = 'finish_flag'
        fin = finish_msg()
        return fin


    def filtered_gen_func():

        comp_dict = dict()
        comp_tms_dict = dict()
        glob_rm_bn = set()
        while True:

            ## 최소 시간 이벤트 내보내기

            for bn in market_coin_bundle_dict.keys():
                if (bn in comp_tms_dict.keys()):
                    continue
                try:
                    frag = next(market_coin_bundle_dict[bn])
                    comp_dict.update({bn:frag})
                    comp_tms_dict.update({bn:frag.tms})
                except:
                    glob_rm_bn.add(bn)

            ## 이벤트 데이터 모두 소진시 루프 종료

            if len(market_coin_bundle_dict)==len(glob_rm_bn):
                while True:
                    if len(comp_tms_dict)==0:
                        class finish_msg():
                            def __init__(self):
                                self.type = 'finish_flag'
                        fin = finish_msg()
                        yield fin
                        return fin
                    mtk = min(comp_tms_dict, key=comp_tms_dict.get)
                    comp_tms_dict.pop(mtk)
                    yield comp_dict.pop(mtk)

            mtk = min(comp_tms_dict,key=comp_tms_dict.get)
            comp_tms_dict.pop(mtk)
            msg = comp_dict.pop(mtk)
            if (msg.tms < start_ts_str) | (msg.tms >= end_ts_str):
                pass
            else:
                yield msg

    return filtered_gen_func()


def get_single_market_coin_event_data_merged_file(market, coin, start_date, end_date, input_dir_path, output_dir_path, pb_cls=cdformat_pb2.coinevent):

    to_do_candidate_list = generate_day_list(start_date=start_date,end_date=end_date)

    file_list = glob.glob(input_dir_path+'\\{}_{}_****-**-**.gz'.format(market,coin))
    to_do_file_path_list = []
    to_do_date_list = []

    for fp in file_list:
        file_date = fp.split('{}_{}_'.format(market,coin))[-1][:-3]
        if file_date in to_do_candidate_list:
            to_do_date_list.append(file_date)
            to_do_file_path_list.append(fp)

    min_date = min(to_do_date_list)
    max_date = max(to_do_date_list)

    output_file_name = '{}_{}_{}_{}.gz'.format(market,coin,min_date.replace('-',''),max_date.replace('-',''))
    output_file_path = output_dir_path+'\\{}'.format(output_file_name)

    with stream.open(output_file_path,'a') as output_stream_file:

        for cur_date in to_do_date_list:
            input_file_name = '{}_{}_{}.gz'.format(market, coin, cur_date)
            input_file_path = '{}\\{}'.format(input_dir_path,input_file_name)

            input_stream_file = stream.parse(ifp=input_file_path, pb_cls=pb_cls)
            for x in input_stream_file:
                output_stream_file.write(x)

    print('Merging Files / Completed / File Name : {}'.format(output_file_name))






"""
## Deprecated

def get_coin_tick_price_df(market,coin,start_datetime,end_datetime,input_dir_path,pb_class=cdformat_pb2.coinevent):

    start_date = start_datetime.split('T')[0]
    end_date = end_datetime.split('T')[0]

    to_do_candidate_list = generate_day_list(start_date=start_date,end_date=end_date)

    file_list = glob.glob(input_dir_path+'\\{}_{}_****-**-**.gz'.format(market,coin))
    to_do_file_path_list = []
    to_do_date_list = []

    for fp in file_list:
        file_date = fp.split('{}_{}_'.format(market,coin))[-1][:-3]
        if file_date in to_do_candidate_list:
            to_do_date_list.append(file_date)
            to_do_file_path_list.append(fp)


    for i in range(len(to_do_date_list)):
        input_file_name = '{}_{}_{}.gz'.format(market, coin, to_do_date_list[i])
        input_file_path = '{}\\{}'.format(input_dir_path,input_file_name)

        input_stream_file = stream.parse(ifp=input_file_path, pb_cls=pb_class)
        k=0
        for x in input_stream_file:

            if (i==0)&(k==0):
                js_frag = MessageToJson(x)
                df_frag = json_normalize(json.loads(js_frag))
                result_df_columns = list(df_frag.columns)
                result_df = pd.DataFrame(columns=result_df_columns)

            js_frag = MessageToJson(x)
            df_frag = json_normalize(json.loads(js_frag))
            result_df = result_df.append(df_frag)

            k+=1

        input_stream_file.close()

    result_df['tms'] = result_df['tms'].map(lambda x:timestamp_into_kst_time(float(x)/1000).replace(tzinfo=None).strftime('%Y-%m-%dT%H:%M:%S'))
    result_df = result_df[(result_df['tms']>=start_datetime)&(result_df['tms']<end_datetime)]
    result_df = result_df.reset_index(drop=True)

    return result_df


def get_merged_coin_tick_price_data_file(market, coin, start_date, end_date, input_dir_path, output_dir_path, pb_cls=cpformat_pb2.CoinPrice):

    to_do_candidate_list = generate_day_list(start_date=start_date,end_date=end_date)

    file_list = glob.glob(input_dir_path+'\\{}_{}_****-**-**.gz'.format(market,coin))
    to_do_file_path_list = []
    to_do_date_list = []

    for fp in file_list:
        file_date = fp.split('{}_{}_'.format(market,coin))[-1][:-3]
        if file_date in to_do_candidate_list:
            to_do_date_list.append(file_date)
            to_do_file_path_list.append(fp)

    min_date = min(to_do_date_list)
    max_date = max(to_do_date_list)

    output_file_name = '{}_{}_{}_{}.gz'.format(market,coin,min_date.replace('-',''),max_date.replace('-',''))
    output_file_path = output_dir_path+'\\{}'.format(output_file_name)

    with stream.open(output_file_path,'a') as output_stream_file:

        for cur_date in to_do_date_list:
            input_file_name = '{}_{}_{}.gz'.format(market, coin, cur_date)
            input_file_path = '{}\\{}'.format(input_dir_path,input_file_name)

            input_stream_file = stream.parse(ifp=input_file_path, pb_cls=pb_cls)
            for x in input_stream_file:
                output_stream_file.write(x)

    print('Merging Files / Completed / File Name : {}'.format(output_file_name))


def load_coin_tick_price_data_on_the_date_generator(market, coin, date, file_dir_path, pb_cls=cpformat_pb2.CoinPrice):

    file_name = '{}_{}_{}.gz'.format(market, coin, date)
    data_file_path = '{}\\{}'.format(file_dir_path, file_name)
    f = stream.parse(ifp=data_file_path, pb_cls=pb_cls)

    return f


def load_coin_tick_price_data_generator(market, coin, start_dt_str, end_dt_str, file_dir_path, pb_cls=cpformat_pb2.CoinPrice):

    start_date = start_dt_str.split('T')[0]
    end_date = end_dt_str.split('T')[0]

    start_ts_str = str(int(datetime.strptime(start_dt_str,'%Y-%m-%dT%H:%M:%S').timestamp()*1000))
    end_ts_str = str(int(datetime.strptime(end_dt_str,'%Y-%m-%dT%H:%M:%S').timestamp()*1000))

    to_do_candidate_list = generate_day_list(start_date=start_date,end_date=end_date)

    file_name = '{}_{}_****-**-**.gz'.format(market,coin)
    file_path = '{}\\{}'.format(file_dir_path,file_name)
    file_list = glob.glob(file_path)
    to_do_file_path_list = []
    to_do_date_list = []

    for fp in file_list:
        file_date = fp.split('{}_{}_'.format(market,coin))[-1][:-3]
        if file_date in to_do_candidate_list:
            to_do_date_list.append(file_date)
            to_do_file_path_list.append(fp)

    to_do_file_path_list.sort(reverse=False)

    for fp_inx in range(len(to_do_file_path_list)):
        if fp_inx==0:
            fp = to_do_file_path_list[fp_inx]
            t = stream.parse(ifp=fp, pb_cls=pb_cls)
            continue
        fp = to_do_file_path_list[fp_inx]
        f = stream.parse(ifp=fp, pb_cls=pb_cls)
        t = chain(t,f)

    def filtered_gen_func():
        for x in t:
            if (x.tms < start_ts_str)|(x.tms >= end_ts_str):
                continue
            yield x

    return filtered_gen_func()

def get_ohlcv_df_from_tick_price_data_generator(generator,time_interval,start_dt_str,end_dt_str):

    k = 0

    for msg in generator:

        cur_datetime = timestamp_into_kst_time(float(msg.tms) / 1000).replace(tzinfo=None)
        cur_dt_str = cur_datetime.strftime('%Y-%m-%dT%H:%M:%S')

        ## Start Datetime 이후부터 시작 & End Datetime 이전까지만

        if (cur_dt_str<start_dt_str)|(cur_dt_str>=end_dt_str):
            continue

        js_frag = json.loads(MessageToJson(message=msg))
        df_frag = json_normalize(js_frag)
        df_frag.index = [cur_datetime]
        df_frag = df_frag.resample(rule=time_interval).first()
        cur_datetime = df_frag.index[0]

        ## 맨처음 루프시에는 리스트 생성

        if k == 0:
            prior_datetime = cur_datetime
            ohlc_result_list = list()
            ohlc_df_frag_list = list()
            k += 1

        ## 계속된 루프시 루틴

        if prior_datetime == cur_datetime:
            ohlc_df_frag_list.append(df_frag)
            continue
        else:
            ## ohlc 완성
            if len(ohlc_df_frag_list) == 0:
                continue
            elif len(ohlc_df_frag_list) == 1:
                row = ohlc_df_frag_list[0].iloc[0]
                ohlc_dict = {'datetime': row.name.strftime('%Y-%m-%dT%H:%M:%S'), 'open': row['price'], 'high': row['price'], 'low': row['price'], 'close': row['price'],'mean':row['price'],'median':row['price'], 'volume': row['volume']}
                ohlc_json = json.dumps(ohlc_dict)

            else:
                merged_ohlc_df_frag = pd.concat(ohlc_df_frag_list)
                o = merged_ohlc_df_frag['price'].iloc[0]
                h = merged_ohlc_df_frag['price'].max()
                l = merged_ohlc_df_frag['price'].min()
                c = merged_ohlc_df_frag['price'].iloc[-1]
                mn = merged_ohlc_df_frag['price'].mean()
                md = merged_ohlc_df_frag['price'].median()
                v = merged_ohlc_df_frag['volume'].sum()
                ohlc_dict = {'datetime': merged_ohlc_df_frag.iloc[0].name.strftime('%Y-%m-%dT%H:%M:%S'), 'open': o, 'high': h, 'low': l, 'close': c,'mean':mn,'median':md, 'volume': v}
                ohlc_json = json.dumps(ohlc_dict)

            ## 리셋

            ohlc_df_frag_list = list()
            ohlc_df_frag_list.append(df_frag)

            ## 데이터 처리 & 모니터링
            # print(ohlc_json)
            ohlc_result_list.append(ohlc_json)

        prior_datetime = cur_datetime
        k += 1

    ## ohlc 완성
    if len(ohlc_df_frag_list) == 0:
        pass
    elif len(ohlc_df_frag_list) == 1:
        row = ohlc_df_frag_list[0].iloc[0]
        ohlc_dict = {'datetime': row.name.strftime('%Y-%m-%dT%H:%M:%S'), 'open': row['price'], 'high': row['price'], 'low': row['price'], 'close': row['price'],'mean':row['price'],'median':row['price'], 'volume': row['volume']}
        ohlc_json = json.dumps(ohlc_dict)
        ohlc_result_list.append(ohlc_json)
    else:
        merged_ohlc_df_frag = pd.concat(ohlc_df_frag_list)
        o = merged_ohlc_df_frag['price'].iloc[0]
        h = merged_ohlc_df_frag['price'].max()
        l = merged_ohlc_df_frag['price'].min()
        c = merged_ohlc_df_frag['price'].iloc[-1]
        mn = merged_ohlc_df_frag['price'].mean()
        md = merged_ohlc_df_frag['price'].median()
        v = merged_ohlc_df_frag['volume'].sum()
        ohlc_dict = {'datetime': merged_ohlc_df_frag.iloc[0].name.strftime('%Y-%m-%dT%H:%M:%S'),'open': o, 'high': h, 'low': l, 'close': c,'mean':mn,'median':md, 'volume': v}
        ohlc_json = json.dumps(ohlc_dict)
        ohlc_result_list.append(ohlc_json)

    ohlc_result_df = pd.DataFrame([json.loads(x) for x in ohlc_result_list]).reset_index(drop=True)

    return ohlc_result_df

"""

###########################################################################
##                          KStock Data File 다루기
###########################################################################

def load_single_market_stock_event_data_generator(market,ticker, start_dt_str, end_dt_str, db_path, pb_cls=kstdformat_pb2.kstevent):

    start_date = start_dt_str.split('T')[0]
    end_date = end_dt_str.split('T')[0]

    start_ts_str = str(int(datetime.strptime(start_dt_str,'%Y-%m-%dT%H:%M:%S').timestamp()))
    end_ts_str = str(int(datetime.strptime(end_dt_str,'%Y-%m-%dT%H:%M:%S').timestamp()))

    to_do_candidate_list = generate_day_list(start_date=start_date,end_date=end_date)

    file_name = '{}_{}_****-**-**.gz'.format(market,ticker)
    file_path = '{}\\{}\\{}\\{}'.format(db_path,market,ticker,file_name)
    file_list = glob.glob(file_path)
    to_do_file_path_list = []
    to_do_date_list = []

    for fp in file_list:
        file_date = fp.split('{}_{}_'.format(market,ticker))[-1][:-3]
        if file_date in to_do_candidate_list:
            to_do_date_list.append(file_date)
            to_do_file_path_list.append(fp)

    to_do_file_path_list.sort(reverse=False)

    for fp_inx in range(len(to_do_file_path_list)):
        if fp_inx==0:
            fp = to_do_file_path_list[fp_inx]
            t = stream.parse(ifp=fp, pb_cls=pb_cls)
            continue
        fp = to_do_file_path_list[fp_inx]
        f = stream.parse(ifp=fp, pb_cls=pb_cls)
        t = chain(t,f)

    def filtered_gen_func():
        for x in t:
            if (x.tms < start_ts_str)|(x.tms >= end_ts_str):
                continue
            yield x

    return filtered_gen_func()


def load_single_market_stock_event_data_bundle(market, ticker, start_dt_str, end_dt_str, db_path, pb_cls=kstdformat_pb2.kstevent):

    start_date = start_dt_str.split('T')[0]
    end_date = end_dt_str.split('T')[0]

    start_ts_str = str(int(datetime.strptime(start_dt_str,'%Y-%m-%dT%H:%M:%S').timestamp()))
    end_ts_str = str(int(datetime.strptime(end_dt_str,'%Y-%m-%dT%H:%M:%S').timestamp()))

    to_do_candidate_list = generate_day_list(start_date=start_date,end_date=end_date)

    file_name = '{}_{}_****-**-**.gz'.format(market,ticker)
    file_path = '{}\\{}\\{}\\{}'.format(db_path,market,ticker,file_name)
    file_list = glob.glob(file_path)
    to_do_file_path_list = []
    to_do_date_list = []

    for fp in file_list:
        file_date = fp.split('{}_{}_'.format(market,ticker))[-1][:-3]
        if file_date in to_do_candidate_list:
            to_do_date_list.append(file_date)
            to_do_file_path_list.append(fp)

    to_do_file_path_list.sort(reverse=False)

    for fp_inx in range(len(to_do_file_path_list)):
        if fp_inx==0:
            fp = to_do_file_path_list[fp_inx]
            t = stream.parse(ifp=fp, pb_cls=pb_cls)
            continue
        fp = to_do_file_path_list[fp_inx]
        f = stream.parse(ifp=fp, pb_cls=pb_cls)
        t = chain(t,f)

    return t


def load_combined_stock_event_data_generator(market_ticker_comb, start_dt_str, end_dt_str, db_path, pb_cls=kstdformat_pb2.kstevent):

    start_ts_str = str(int(datetime.strptime(start_dt_str,'%Y-%m-%dT%H:%M:%S').timestamp()))
    end_ts_str = str(int(datetime.strptime(end_dt_str,'%Y-%m-%dT%H:%M:%S').timestamp()))

    market_ticker_bundle_dict = dict()
    bn = 0
    for m in market_ticker_comb.keys():
        for c in market_ticker_comb[m]:
            try:
                mcfg = load_single_market_stock_event_data_bundle(market=m,ticker=c,start_dt_str=start_dt_str,end_dt_str=end_dt_str,db_path=db_path)
                market_ticker_bundle_dict.update({bn:mcfg})
                bn+=1
            except:
                print('{} / {} / {} / {} /데이터가 존재하지 않습니다'.format(m,c,start_dt_str,end_dt_str))
                continue

    if len(market_ticker_bundle_dict)==0:
        print('데이터가 존재하지 않습니다'.format(m, c, start_dt_str, end_dt_str))

        class finish_msg():
            def __init__(self):
                self.type = 'finish_flag'
        fin = finish_msg()
        return fin


    def filtered_gen_func():

        comp_dict = dict()
        comp_tms_dict = dict()
        glob_rm_bn = set()
        while True:

            ## 최소 시간 이벤트 내보내기

            for bn in market_ticker_bundle_dict.keys():
                if (bn in comp_tms_dict.keys()):
                    continue
                try:
                    frag = next(market_ticker_bundle_dict[bn])
                    comp_dict.update({bn:frag})
                    comp_tms_dict.update({bn:frag.tms})
                except:
                    glob_rm_bn.add(bn)

            ## 이벤트 데이터 모두 소진시 루프 종료

            if len(market_ticker_bundle_dict)==len(glob_rm_bn):
                while True:
                    if len(comp_tms_dict)==0:
                        class finish_msg():
                            def __init__(self):
                                self.type = 'finish_flag'
                        fin = finish_msg()
                        yield fin
                        return fin
                    mtk = min(comp_tms_dict, key=comp_tms_dict.get)
                    comp_tms_dict.pop(mtk)
                    yield comp_dict.pop(mtk)

            mtk = min(comp_tms_dict,key=comp_tms_dict.get)
            comp_tms_dict.pop(mtk)
            msg = comp_dict.pop(mtk)
            if (msg.tms < start_ts_str) | (msg.tms >= end_ts_str):
                pass
            else:
                yield msg

    return filtered_gen_func()


def get_single_market_stock_event_data_merged_file(market, ticker, start_date, end_date, input_dir_path, output_dir_path, pb_cls=kstdformat_pb2.kstevent):

    to_do_candidate_list = generate_day_list(start_date=start_date,end_date=end_date)

    file_list = glob.glob(input_dir_path+'\\{}_{}_****-**-**.gz'.format(market,ticker))
    to_do_file_path_list = []
    to_do_date_list = []

    for fp in file_list:
        file_date = fp.split('{}_{}_'.format(market,ticker))[-1][:-3]
        if file_date in to_do_candidate_list:
            to_do_date_list.append(file_date)
            to_do_file_path_list.append(fp)

    min_date = min(to_do_date_list)
    max_date = max(to_do_date_list)

    output_file_name = '{}_{}_{}_{}.gz'.format(market,ticker,min_date.replace('-',''),max_date.replace('-',''))
    output_file_path = output_dir_path+'\\{}'.format(output_file_name)

    with stream.open(output_file_path,'a') as output_stream_file:

        for cur_date in to_do_date_list:
            input_file_name = '{}_{}_{}.gz'.format(market, ticker, cur_date)
            input_file_path = '{}\\{}'.format(input_dir_path,input_file_name)

            input_stream_file = stream.parse(ifp=input_file_path, pb_cls=pb_cls)
            for x in input_stream_file:
                output_stream_file.write(x)

    print('Merging Files / Completed / File Name : {}'.format(output_file_name))






def load_stock_ohlc_price_data_on_the_date_generator(market, ticker, date, root_dir_path, pb_cls=kstpohlc_pb2.KSTPrice):

    file_name = '{}_{}_{}.gz'.format(market, ticker, date)
    data_file_path = '{}\\KRX\\{}\\{}'.format(root_dir_path, ticker, file_name)
    f = stream.parse(ifp=data_file_path, pb_cls=pb_cls)

    return f

def load_stock_universe_data_generator(market, start_dt_str, end_dt_str, root_dir_path):

    start_date = start_dt_str.split('T')[0]
    end_date = end_dt_str.split('T')[0]

    to_do_candidate_list = generate_day_list(start_date=start_date,end_date=end_date)

    file_name = '{}_Universe_****-**-**.csv'.format(market)
    file_path = '{}\\{}_Universe\\{}'.format(root_dir_path,market,file_name)
    file_list = glob.glob(file_path)

    output_columns = ['market','code','ticker','name','market_cap']

    result_df = pd.DataFrame(columns=output_columns)

    for fp in file_list:
        file_date = fp.split('{}_Universe_'.format(market))[-1][:-4]
        if file_date in to_do_candidate_list:
            df_frag = pd.read_csv(filepath_or_buffer=fp,index_col=[0],encoding='euc_kr')
            df_frag = df_frag[output_columns]


            result_df = result_df.append(df_frag).drop_duplicates(subset=['ticker']).sort_values(by=['ticker']).reset_index(drop=True)

    return result_df



def load_stock_ohlc_price_data_generator(market, ticker, start_dt_str, end_dt_str, root_dir_path, pb_cls=kstpohlc_pb2.KSTPrice):

    start_date = start_dt_str.split('T')[0]
    end_date = end_dt_str.split('T')[0]

    to_do_candidate_list = generate_day_list(start_date=start_date,end_date=end_date)

    file_name = '{}_{}_****-**-**.gz'.format(market,ticker)
    file_path = '{}\\{}\\{}\\{}'.format(root_dir_path,market,ticker,file_name)
    file_list = glob.glob(file_path)
    to_do_file_path_list = []
    to_do_date_list = []

    for fp in file_list:
        file_date = fp.split('{}_{}_'.format(market,ticker))[-1][:-3]
        if file_date in to_do_candidate_list:
            to_do_date_list.append(file_date)
            to_do_file_path_list.append(fp)

    to_do_file_path_list.sort(reverse=False)

    for fp_inx in range(len(to_do_file_path_list)):
        if fp_inx==0:
            fp = to_do_file_path_list[fp_inx]
            t = stream.parse(ifp=fp, pb_cls=pb_cls)
            continue
        fp = to_do_file_path_list[fp_inx]
        f = stream.parse(ifp=fp, pb_cls=pb_cls)
        t = chain(t,f)


    def filtered_gen_func():
        for x in t:
            if (x.dt >= start_dt_str)&(x.dt < end_dt_str):
                yield x
    return filtered_gen_func()


def get_stock_ohlc_df(market, ticker, start_dt_str, end_dt_str,time_interval, root_dir_path, pb_cls=kstpohlc_pb2.KSTPrice):

    start_date = start_dt_str.split('T')[0]
    end_date = end_dt_str.split('T')[0]

    to_do_candidate_list = generate_day_list(start_date=start_date,end_date=end_date)

    file_name = '{}_{}_****-**-**.gz'.format(market,ticker)
    file_path = '{}\\{}\\{}\\{}'.format(root_dir_path,market,ticker,file_name)
    file_list = glob.glob(file_path)
    to_do_file_path_list = []
    to_do_date_list = []

    for fp in file_list:
        file_date = fp.split('{}_{}_'.format(market,ticker))[-1][:-3]
        if file_date in to_do_candidate_list:
            to_do_date_list.append(file_date)
            to_do_file_path_list.append(fp)

    to_do_file_path_list.sort(reverse=False)


    for fp_inx in range(len(to_do_file_path_list)):
        if fp_inx==0:
            fp = to_do_file_path_list[fp_inx]
            t = stream.parse(ifp=fp, pb_cls=pb_cls)
            continue
        fp = to_do_file_path_list[fp_inx]
        f = stream.parse(ifp=fp, pb_cls=pb_cls)
        t = chain(t,f)

    ## 날짜로 필터링 후 df 만들기

    l=0
    for x in t:
        if (x.dt >= start_dt_str)&(x.dt < end_dt_str):
            msg_js = json.loads(MessageToJson(x))
            try:
                msg_js['volume']
            except:
                # msg_js['volume'] = np.nan
                # msg_js['value'] = np.nan
                continue

            if (l==0):
                result_df = pd.DataFrame([msg_js])

                l=1
                continue

            result_df = result_df.append([msg_js])

    try:
        result_df = result_df.reset_index(drop=True)
    except:
        final_result_df = pd.DataFrame(columns=['dt','market','code','ticker','name','open','high','low','close','volume','value'])
        return final_result_df

    ## 시간프레임 설정

    result_df['dt'] = result_df['dt'].map(lambda x:datetime.strptime(x,'%Y-%m-%dT%H:%M:%S'))
    result_df = result_df.set_index(keys=['dt'])

    # del result_df.index.name

    final_result_df = result_df[['market','code','ticker','name']].resample(rule=time_interval,label='right',closed='right').first().fillna(method='ffill')
    final_result_df['open']=result_df['open'].resample(rule=time_interval,label='right',closed='right').first()
    final_result_df['high']=result_df['high'].resample(rule=time_interval,label='right',closed='right').max()
    final_result_df['low']=result_df['low'].resample(rule=time_interval,label='right',closed='right').min()
    final_result_df['close']=result_df['close'].resample(rule=time_interval,label='right',closed='right').last().fillna(method='ffill')
    final_result_df['volume']=result_df['volume'].resample(rule=time_interval,label='right',closed='right').sum()
    final_result_df['value']=result_df['value'].resample(rule=time_interval,label='right',closed='right').sum()

    final_result_df['open'] = np.where(final_result_df['volume'] <= 0, final_result_df['close'], final_result_df['open'])
    final_result_df['high'] = np.where(final_result_df['volume'] <= 0, final_result_df['close'], final_result_df['high'])
    final_result_df['low'] = np.where(final_result_df['volume'] <= 0, final_result_df['close'], final_result_df['low'])

    # final_result_df.index.name = 'dt'

    final_result_df = final_result_df.reset_index(drop=False)

    final_result_df['dt'] = final_result_df['dt'].map(lambda x:x.strftime('%Y-%m-%dT%H:%M:%S'))

    return final_result_df



###########################################################################
##                        시뮬레이션 관련
###########################################################################

class hexpo_sim_trader():
    def __init__(self,rcv_port,snd_port,spd_port):
        self.rcv_port = rcv_port
        self.snd_port = snd_port
        self.spd_port = spd_port

        self.port_settings()

    def port_settings(self):
        self.context = zmq.Context()

        self.data_receiver = self.context.socket(zmq.SUB)
        self.data_receiver.connect("tcp://127.0.0.1:{}".format(self.rcv_port))
        self.data_receiver.setsockopt_string(zmq.SUBSCRIBE,"")

        self.data_sender = self.context.socket(zmq.PUB)
        self.data_sender.bind("tcp://127.0.0.1:{}".format(self.snd_port))

        self.speed_reporter = self.context.socket(zmq.PUB)
        self.speed_reporter.bind("tcp://127.0.0.1:{}".format(self.spd_port))

    def default_settings(self):
        pass

    def main(self):
        pass


class hexpo_sim_mltrainer():
    def __init__(self,rcv_port,spd_port):
        self.rcv_port = rcv_port
        # self.snd_port = snd_port
        self.spd_port = spd_port

        self.port_settings()

    def port_settings(self):
        self.context = zmq.Context()

        self.data_receiver = self.context.socket(zmq.SUB)
        self.data_receiver.connect("tcp://127.0.0.1:{}".format(self.rcv_port))
        self.data_receiver.setsockopt_string(zmq.SUBSCRIBE,"")

        # self.data_sender = self.context.socket(zmq.PUB)
        # self.data_sender.bind("tcp://127.0.0.1:{}".format(self.snd_port))

        self.speed_reporter = self.context.socket(zmq.PUB)
        self.speed_reporter.bind("tcp://127.0.0.1:{}".format(self.spd_port))

    def default_settings(self):
        pass

    def main(self):
        pass



class hexpo_sim_trader_kstock():
    def __init__(self,root_dir_path,snd_port):

        self.root_dir_path = root_dir_path
        self.snd_port = snd_port


        self.port_settings()

    def port_settings(self):
        self.context = zmq.Context()

        # self.data_sender = self.context.socket(zmq.PUB)
        self.data_sender = self.context.socket(zmq.PUSH)
        self.data_sender.connect("tcp://127.0.0.1:{}".format(self.snd_port))

    def default_settings(self):
        pass

    def main(self):
        pass


###########################################################################
##                          트레이딩 관련
###########################################################################

class hexpo_trader():
    def __init__(self,rcv_port, snd_port, lvtst_trd_rcv_port,lvtst_orb_rcv_port):

        self.rcv_port = rcv_port
        self.snd_port = snd_port
        self.lvtst_trd_rcv_port = lvtst_trd_rcv_port
        self.lvtst_orb_rcv_port = lvtst_orb_rcv_port

        self.port_settings()

    def port_settings(self):
        self.context = zmq.Context()

        self.data_receiver = self.context.socket(zmq.SUB)
        self.data_receiver.connect("tcp://127.0.0.1:{}".format(self.rcv_port))
        self.data_receiver.setsockopt_string(zmq.SUBSCRIBE,"")

        self.data_sender = self.context.socket(zmq.PUB)
        self.data_sender.bind("tcp://127.0.0.1:{}".format(self.snd_port))

    def live_test_trade_data_streaming_start(self):

        self.lvtst_trd_receiver = self.context.socket(zmq.SUB)
        self.lvtst_trd_receiver.connect("tcp://127.0.0.1:{}".format(self.lvtst_trd_rcv_port))
        self.lvtst_trd_receiver.setsockopt_string(zmq.SUBSCRIBE,"")

    def live_test_orderbook_data_streaming_start(self):

        self.lvtst_orb_receiver = self.context.socket(zmq.SUB)
        self.lvtst_orb_receiver.connect("tcp://127.0.0.1:{}".format(self.lvtst_orb_rcv_port))
        self.lvtst_orb_receiver.setsockopt_string(zmq.SUBSCRIBE, "")

    def live_test_trade_data_streaming_close(self):
        self.lvtst_trd_receiver.close()

    def live_test_orderbook_data_streaming_close(self):
        self.lvtst_orb_receiver.close()

    def default_settings(self):
        pass

    def main(self):
        pass
