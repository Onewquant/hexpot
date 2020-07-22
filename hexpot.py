###########################################
## System, File, Data Structure 관련 import
###########################################

import os
import sys
import glob
import shutil
from itertools import chain, permutations, combinations

###########################################
##         Math, 자료형 관련 import
###########################################

import csv
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
import threading

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

from hexpot.hexpot_pb import cdformat_pb2, kstdformat_pb2, kfinformat_pb2
from google.protobuf.json_format import MessageToJson
import stream

"""
Protocol Buffer 사용법

## Message <-> Json

# pb_cls = kstdformat_pb2.kstevent()
# 
# Message To Json // MessageToJson(message)
# Json To Message // 각 요소들 알아서 매핑

## Message <-> String

# pb_cls = kstdformat_pb2.kstevent()
# 
# serialization // message.SerializeToString()
# deserialization // pb_cls.FromString(ser_str)
"""

###########################################
##                 AWS S3
###########################################

import boto3
import botocore
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
import logging

###########################################
##                 ML
###########################################

import pickle
from pykalman import KalmanFilter

###########################################################################
##                          General Tools
###########################################################################

## 프로그램 작동 시간 측정 데코레이터
"""
opening_time = time.time()
closing_time = time.time()
print(closing_time-opening_time)
"""

def timestamp_into_kst_time(ts):
    KST = pytz.timezone('Asia/Seoul')
    try:
        dt = datetime.utcfromtimestamp(float(ts))
        k_dt = pytz.utc.localize(dt).astimezone(KST)
        return k_dt
    except:
        return np.nan


def timestamp_into_dt_str_format(tms, format='%Y-%m-%dT%H:%M:%S.%f'):
    return timestamp_into_kst_time(tms).strftime(format)


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
##                          KFinance Data File
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
