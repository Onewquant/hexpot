import pandas as pd
from datetime import datetime, timedelta
import time
import requests
import json
import math
import numpy as np

###########################################################################
##                  Indicators
###########################################################################

## 상위 시간단위 ohlcv df

def get_upper_timeframe_ohlcv_df(df,time_interval):
    df_upper = df[['open', 'high', 'low', 'close', 'volume']]
    df_upper.index = df['datetime'].map(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S'))

    o = df_upper.resample(time_interval).first()['open']
    h = df_upper.resample(time_interval).max()['high']
    l = df_upper.resample(time_interval).min()['low']
    c = df_upper.resample(time_interval).last()['close']
    v = df_upper.resample(time_interval).sum()['volume']
    df_upper = pd.concat([o, h, l, c, v], axis=1).reset_index(drop=False)
    df_upper['datetime'] = df_upper['datetime'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S'))
    return df_upper

## 거래량, 거래금액 관련

def trd_volume_dataframe(df,column='close'):
    dataframe = df.copy()
    volume = df['volume']
    price = df[column]

    volume_trend = volume - volume.shift(1)
    trd_amount = price * volume
    trd_amount_trend = trd_amount - trd_amount.shift(1)

    window_list = [3,5,20]
    for w in window_list:
        dataframe['volume_ma%s'%w] = volume.rolling(center=False, window=w).mean()

    dataframe['volume_trend'] = volume_trend
    dataframe['trd_amount'] = trd_amount
    dataframe['trd_amount_trend'] = trd_amount_trend

    return dataframe

## 누적 수익률

def cum_rets_dataframe(df,column='Close_'):
    dataframe = df.copy()
    close_price = df[column]
    cumrets_series = (1 + ((close_price - close_price.shift(1)) / close_price).fillna(0)).cumprod()
    dataframe['Cum_Rets'] = cumrets_series
    return dataframe

## 단순 이동평균선

def moving_average_dataframe(df,column='close',window_list=[5,20,50,100]):
    dataframe = df.copy()
    for window in window_list:
        dataframe['ma%s'%window] = dataframe[column].rolling(center=False, window=window).mean()
    return dataframe

## 지수 이동평균선

def exponential_moving_average_series(df,column='close',window=20):
    weight = float(2)/(window+1) # 지수이동평균가중치
    result = list()
    for inx,row in df.iterrows():
        value = row[column]
        if not result: # price_df.iloc[0]은 계산하지 않음
            result.append(value)
        else:
            result.append((value*weight)+(result[-1]*(1-weight)))

    result = pd.Series(data=result,index=list(df.index))
    return result

def exponential_moving_average_dataframe(df,column='close',window=20):
    dataframe = df.copy()
    ema_series = exponential_moving_average_series(df=dataframe,column=column,window=window)
    dataframe['ema'] = ema_series

    return dataframe

## 지수 이동평균선의 추세 지표

def ema_trend_series(df,window=5):

    ds = df['ema']

    ds_tangent = ds - ds.shift(window)
    ds_increase_cond = (ds_tangent >= 0)
    ds_decrease_cond = (ds_tangent < 0)

    ema_trend_series = (ds_increase_cond*1 + ds_decrease_cond*(-1))

    return ema_trend_series

def ema_trend_dataframe(df,window=5):
    dataframe = df.copy()
    dataframe['ema_trend'] = ema_trend_series(df=df,window=window)

    return dataframe

## 평균 이동평균선 스코어 (단순 이평선 이용)

def average_moving_average_score_dataframe(df,column = 'close',window_list = [5,20,60,100]):
    dataframe = df.copy()
    cum_ma_series = pd.Series(index=dataframe.index).fillna(0)
    for w in window_list:
        cum_ma_series+=(dataframe[column] > dataframe[column].rolling(center=False,window=w).mean())
    ama_series = cum_ma_series/len(window_list)
    dataframe['AMA_Score'] = ama_series
    return dataframe

## Envelop 채널지표 (지수 이동평균선이 중심선)

def channel_dataframe(df,window=20,column = 'close',coefficient=0.05):
    dataframe = df.copy()
    ema = exponential_moving_average_series(df=df,column=column,window=window)

    dataframe['upper_channel'] = (1+coefficient)*ema
    dataframe['lower_channel'] = (1-coefficient)*ema
    return dataframe

## 볼린저밴드 채널지표 (20일 단순이동평균선이 중심선)

def bollinger_band_dataframe(df,column='close',window=20):
    dataframe = df.copy()
    std_series = dataframe[column].rolling(center=False,window=window).std()
    ma_series = dataframe[column].rolling(center=False,window=window).mean()

    dataframe['upper_sbollinger'] = ma_series + 2*std_series
    dataframe['lower_sbollinger'] = ma_series - 2*std_series
    return dataframe

## 변형 볼린저밴드 채널 지표 (지수이동평균선을 중심선으로 함)

def modified_bollinger_band_dataframe(df,column='close',window=20):
    dataframe = df.copy()
    std_series = dataframe[column].rolling(center=False,window=window).std()
    ema_series = exponential_moving_average_series(df=dataframe,column=column,window=window)

    dataframe['upper_bollinger'] = ema_series + 2*std_series
    dataframe['lower_bollinger'] = ema_series - 2*std_series
    dataframe['avg_width_bollinger'] = (dataframe['upper_bollinger'] - dataframe['lower_bollinger']).rolling(center=False, window=window).mean()

    return dataframe


## MACD 지표

def macd_series(df,column='close'):

    fast_macd = exponential_moving_average_series(df=df,column=column,window=12) - exponential_moving_average_series(df=df, column=column, window=26)
    slow_macd = exponential_moving_average_series(df=pd.DataFrame(data=fast_macd), column=0, window=9)
    histogram = fast_macd - slow_macd

    histogram_trend = histogram - histogram.shift(1)

    result = {'fast':fast_macd,'slow':slow_macd,'histogram':histogram,'histogram_trend':histogram_trend}
    return result

def macd_hist_dataframe(df,column='close'):
    dataframe = df.copy()
    macd_dict = macd_series(df=dataframe,column=column)
    dataframe['macd_histogram'] = macd_dict['histogram']
    dataframe['macd_histogram_trend']=macd_dict['histogram_trend']

    return dataframe

## Force Index (강도지수) 지표

def force_index_dataframe(df,column='close',window=2):
    dataframe = df.copy()
    force_index_series = (dataframe[column] - dataframe[column].shift(1)) * dataframe['volume']
    force_index_mean_rolling_series = force_index_series.rolling(center=False,window=250).mean()
    force_index_std_rolling_series = force_index_series.rolling(center=False,window=250).std()
    force_index_ratio = force_index_series / force_index_mean_rolling_series
    force_index_norm = (force_index_series-force_index_mean_rolling_series)/force_index_std_rolling_series

    force_index_fear_rolling_standard = ((((force_index_norm < -2.5) * 1).rolling(center=False,window=3).sum())>0) * 1
    force_index_greed_rolling_standard = ((((force_index_norm >= 2.5) * 1).rolling(center=False, window=3).sum()) > 0) * 1

    dataframe['force_index_fear'] = force_index_fear_rolling_standard
    dataframe['force_index_greed'] = force_index_greed_rolling_standard

    dataframe['force_index_norm'] = force_index_norm
    dataframe['force_index_ratio'] = force_index_ratio

    dataframe['force_index_ddm'] = ((force_index_norm < force_index_norm.shift(1))&(force_index_norm < 0))*1
    dataframe['force_index_dum'] = ((force_index_norm > force_index_norm.shift(1))&(force_index_norm > 0))*1

    return dataframe