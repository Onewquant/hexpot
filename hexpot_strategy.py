
from hexpot.hexpot_indicators import *



class strategy_test():

    def __init__(self):
        self.column_settings()

    ## buy signal generation

    def column_settings(self):
        self.add_header_columns = ['market', 'coin', 'datetime']
        self.add_buy_info_columns = ['trd_id', 'pattern', 'timeframe', 'bs_dt_i', 'bs_dt_r', 'bsg_latency']
        self.add_buy_plan_columns = ['planned_ent_price','planned_lc_price','planned_gc_price','planned_units','initial_risk','ent_band_upper','ent_band_lower','ent_band_width']
        self.add_buy_result_columns = ['ent_dt','ent_order_num','ent_price','units','ent_score']
        self.add_sell_plan_columns = ['ss_dt_i','ss_dt_r','ssg_latency','ext_type','planned_ext_price','ext_band_upper','ext_band_lower','ext_band_width']
        self.add_sell_result_columns = ['ext_dt','ext_order_num','ext_price','trd_result','ret','net_ret','r_value','r_multiple','ext_score','trd_score']

    def buy_signal_generation_main(self, df):

        bsdf,df = self.buy_pattern_recognition(df=df)
        bsdf = self.determine_buy_order_plan_and_info(bsdf=bsdf,df=df)
        return bsdf

    @staticmethod
    def buy_pattern_recognition(df):


        ## 사용지표구성
        df_timeframe_min = int(((datetime.strptime(df.iloc[-1]['datetime'],'%Y-%m-%dT%H:%M:%S') - datetime.strptime(df.iloc[-2]['datetime'],'%Y-%m-%dT%H:%M:%S')).total_seconds())/12)

        upper_df = get_upper_timeframe_ohlcv_df(df=df,time_interval='{}T'.format(df_timeframe_min))
        upper_df = macd_hist_dataframe(df=upper_df)
        upper_df = exponential_moving_average_dataframe(df=upper_df,window=20)
        upper_df = ema_trend_dataframe(df=upper_df,window=1)

        main_df = df.copy()
        main_df = exponential_moving_average_dataframe(df=main_df, window=20)
        main_df = modified_bollinger_band_dataframe(df=main_df,column='close',window=20)

        ## Buy Signal 조건 판별

        scrn0 = ((upper_df['ema_trend']<0)&(upper_df['macd_histogram_trend']<0)).iloc[-1]
        if scrn0:
            return pd.DataFrame(columns=list(main_df.columns)), main_df

        cond0 = (main_df['close'] >= (2*main_df['ema'] + main_df['lower_bollinger']) / 3) & (main_df['close'] < (2 * main_df['ema'] + main_df['upper_bollinger']) / 3)
        cond1 = main_df['volume'] <= 15
        signal_cond = cond0 & cond1

        ## Buy Signal 확인 및 결과 리턴

        bsdf = main_df[signal_cond]

        if (len(bsdf)!=0):
            if (main_df['datetime'].iloc[-1] == bsdf['datetime'].iloc[-1]):
                return bsdf.iloc[-1:].reset_index(drop=True), main_df

        return pd.DataFrame(columns=list(main_df.columns)), main_df


        # ## 사용지표구성
        #
        # upper_df = get_upper_timeframe_ohlcv_df(df=df,time_interval='30T')
        # upper_df = macd_hist_dataframe(df=upper_df)
        # upper_df = exponential_moving_average_dataframe(df=upper_df,window=20)
        # upper_df = ema_trend_dataframe(df=upper_df,window=1)
        #
        # main_df = df.copy()
        # main_df = exponential_moving_average_dataframe(df=main_df, window=20)
        # main_df = modified_bollinger_band_dataframe(df=main_df,column='close',window=20)
        #
        # ## Buy Signal 조건 판별
        #
        # # scrn0 = ((upper_df['ema_trend']!=-1)&(upper_df['macd_histogram_trend']>=0)).iloc[-1]
        # # if ~scrn0:
        # #     return pd.DataFrame(columns=list(main_df.columns)), main_df
        #
        # cond0 = (main_df['close']-main_df['open'] >= 0)
        # cond1 = main_df['volume'] > 0
        # signal_cond = cond0 & cond1
        #
        # ## Buy Signal 확인 및 결과 리턴
        #
        # bsdf = main_df[signal_cond]
        #
        # if (len(bsdf)!=0):
        #     if (main_df['datetime'].iloc[-1] == bsdf['datetime'].iloc[-1]):
        #         return bsdf.iloc[-1:].reset_index(drop=True), main_df
        #
        # return pd.DataFrame(columns=list(main_df.columns)), main_df


    def determine_buy_order_plan_and_info(self,bsdf, df):

        ## Columns

        add_rest_columns =  list(set(bsdf.columns) - set(self.add_header_columns))
        add_rest_columns.sort(reverse=False)
        total_columns = self.add_header_columns + self.add_buy_info_columns + self.add_buy_plan_columns + add_rest_columns

        if len(bsdf)==0:
            return pd.DataFrame(columns=total_columns)

        ## Plan

        # total_trd_eq = 1000000
        fixed_risk_per_trd = 1000

        bsdf['planned_ent_price'] = df.iloc[-1]['close']
        bsdf['planned_lc_price'] = bsdf.iloc[-1]['planned_ent_price'] - (df[['open','close']].min(axis=1)-df['low']).rolling(center=False,window=250).max().iloc[-1]
        bsdf['planned_gc_price'] = math.ceil(df.iloc[-1]['close']*1.1*100)/100
        bsdf['planned_units'] = math.ceil((fixed_risk_per_trd / (bsdf.iloc[-1]['planned_ent_price'] - bsdf.iloc[-1]['planned_lc_price'])*100))/100
        bsdf['initial_risk'] = math.ceil(((bsdf['planned_ent_price'] - bsdf['planned_lc_price'])*bsdf['planned_units']).iloc[-1]*100)/100
        bsdf['ent_band_upper'] = math.ceil(df.iloc[-1]['upper_bollinger']*100)/100
        bsdf['ent_band_lower'] = math.ceil(df.iloc[-1]['lower_bollinger']*100)/100
        bsdf['ent_band_width'] = math.ceil(df.iloc[-1]['avg_width_bollinger']*100)/100

        ## Info

        time_now = datetime.now()
        time_now_str = time_now.strftime('%Y-%m-%dT%H:%M:%S.%f')
        last_df_dt = datetime.strptime(df.iloc[-1]['datetime'],'%Y-%m-%dT%H:%M:%S')
        df_timeframe_seconds = (last_df_dt - datetime.strptime(df.iloc[-2]['datetime'],'%Y-%m-%dT%H:%M:%S')).total_seconds()
        bs_time_i_dt = last_df_dt + timedelta(seconds=df_timeframe_seconds)
        bs_time_i_str = bs_time_i_dt.strftime('%Y-%m-%dT%H:%M:%S.%f')

        bsdf['pattern'] = 'ptest'
        bsdf['trd_id'] = '{}{}'.format(bsdf.iloc[-1]['pattern'],time_now_str.replace('-','').replace(':','').replace('.','')[:-3])
        bsdf['timeframe'] = df_timeframe_seconds
        bsdf['bs_dt_i'] = bs_time_i_str
        bsdf['bs_dt_r'] = time_now_str
        bsdf['bsg_latency'] = (time_now - bs_time_i_dt).total_seconds()

        return bsdf[total_columns]


    ## sell signal generation

    def sell_signal_generation_main(self, bsdf, df):
        planned_ext_info, ssdf, df = self.sell_pattern_recognition(bsdf=bsdf,df=df)
        ssdf = self.determine_sell_order_plan_and_info(planned_ext_info=planned_ext_info,ssdf=ssdf,df=df)
        return ssdf

    @staticmethod
    def sell_pattern_recognition(bsdf,df):

        ## 사용지표구성

        bsdf_for_sell = bsdf.copy()
        bsdf_for_sell_row = bsdf_for_sell.iloc[0]

        upper_df = get_upper_timeframe_ohlcv_df(df=df,time_interval='{}T'.format(int(bsdf_for_sell['timeframe']*5/60)))
        upper_df = macd_hist_dataframe(df=upper_df)
        upper_df = exponential_moving_average_dataframe(df=upper_df,window=20)
        upper_df = ema_trend_dataframe(df=upper_df,window=1)

        main_df = df.copy()
        main_df = exponential_moving_average_dataframe(df=main_df, window=20)
        main_df = modified_bollinger_band_dataframe(df=main_df,column='close',window=20)

        ## Sell Signal 조건 판별

        # 스크리닝 조건


        scrn_cond0 = ((upper_df['ema_trend'] < 0) & (upper_df['macd_histogram_trend'] < 0)).iloc[-1]
        scrn_cond_total = scrn_cond0
        if scrn_cond_total:
            ext_type = 'sc'
            planned_ext_price = main_df.iloc[-1]['close']
            planned_ext_info = (ext_type,planned_ext_price)
            return planned_ext_info, bsdf_for_sell, main_df

        # 타임컷 조건

        tc_cond0 = (datetime.now() - datetime.strptime(bsdf_for_sell_row['bs_dt_r'],'%Y-%m-%dT%H:%M:%S.%f')).total_seconds() >= bsdf_for_sell_row['timeframe']*72
        tc_cond_total = tc_cond0
        if tc_cond_total:
            ext_type = 'tc'
            planned_ext_price = main_df.iloc[-1]['close']
            planned_ext_info = (ext_type,planned_ext_price)
            return planned_ext_info, bsdf_for_sell, main_df

        # 익절 조건
        gc_cond0 = main_df['close'] >= ((main_df['ema'] + 7*main_df['upper_bollinger']) / 8)
        gc_cond1 = (3*main_df['volume'] >= max(300,upper_df.iloc[-1]['volume']))
        gc_cond_total = gc_cond0 & gc_cond1
        if gc_cond_total.iloc[-1]:
            ext_type = 'gc'
            planned_ext_price = main_df.iloc[-1]['close']
            planned_ext_info = (ext_type,planned_ext_price)
            return planned_ext_info, bsdf_for_sell, main_df

        # 손절 조건
        # lc_cond0 = main_df['close'] <= ((main_df['ema'] + main_df['lower_bollinger']) / 2)
        lc_cond0 = main_df['close'] - (main_df['lower_bollinger']) < 0
        lc_cond1 = main_df['close'] - bsdf_for_sell_row['ent_price']*0.95 <= 0
        lc_cond_total = lc_cond0 | lc_cond1
        if lc_cond_total.iloc[-1]:
            ext_type = 'lc'
            planned_ext_price = bsdf_for_sell.iloc[-1]['planned_lc_price']
            planned_ext_info = (ext_type,planned_ext_price)
            return planned_ext_info, bsdf_for_sell, main_df


        # 아무 조건에도 속하지 않을 경우
        ext_type = None
        planned_ext_price = np.nan
        planned_ext_info = (ext_type, planned_ext_price)
        return planned_ext_info, pd.DataFrame(columns=list(bsdf.columns)), main_df



    def determine_sell_order_plan_and_info(self,planned_ext_info, ssdf, df):

        ## Columns
        add_rest_columns =  list(set(ssdf.columns) - set(self.add_header_columns + self.add_buy_info_columns + self.add_buy_plan_columns + self.add_buy_result_columns))
        add_rest_columns.sort(reverse=False)
        total_columns = self.add_header_columns + self.add_buy_info_columns + self.add_buy_plan_columns + self.add_buy_result_columns + self.add_sell_plan_columns + add_rest_columns

        if len(ssdf) == 0:
            return pd.DataFrame(columns=total_columns)

        ## Plan

        ss_plan_df = ssdf.copy()
        ss_plan_df['ext_type'] = planned_ext_info[0]
        ss_plan_df['planned_ext_price'] = planned_ext_info[1]
        ss_plan_df['ext_band_upper'] = math.ceil(df.iloc[-1]['upper_bollinger']*100)/100
        ss_plan_df['ext_band_lower'] = math.ceil(df.iloc[-1]['lower_bollinger']*100)/100
        ss_plan_df['ext_band_width'] = math.ceil(df.iloc[-1]['avg_width_bollinger']*100)/100


        ## Info

        time_now = datetime.now()
        time_now_str = time_now.strftime('%Y-%m-%dT%H:%M:%S.%f')
        last_df_dt = datetime.strptime(df.iloc[-1]['datetime'], '%Y-%m-%dT%H:%M:%S')
        df_timeframe_seconds = (last_df_dt - datetime.strptime(df.iloc[-2]['datetime'], '%Y-%m-%dT%H:%M:%S')).total_seconds()
        ss_time_i_dt = last_df_dt + timedelta(seconds=df_timeframe_seconds)
        ss_time_i_str = ss_time_i_dt.strftime('%Y-%m-%dT%H:%M:%S.%f')


        ss_plan_df['ss_dt_i'] = ss_time_i_str
        ss_plan_df['ss_dt_r'] = time_now_str
        ss_plan_df['ssg_latency'] = (time_now - ss_time_i_dt).total_seconds()

        return ss_plan_df[total_columns]






# from hexpot.hexpot_indicators import *
#
#
#
# class strategy_test():
#
#     def __init__(self):
#         pass
#
#     ## buy signal generation
#
#     def buy_signal_generation_main(self, df):
#
#         bsdf,df = self.buy_pattern_recognition(df=df)
#         bsdf = self.determine_buy_order_plan_and_info(bsdf=bsdf,df=df)
#         return bsdf
#
#     @staticmethod
#     def buy_pattern_recognition(df):
#
#         ## 사용지표구성
#
#         upper_df = get_upper_timeframe_ohlcv_df(df=df,time_interval='30T')
#         upper_df = macd_hist_dataframe(df=upper_df)
#         upper_df = exponential_moving_average_dataframe(df=upper_df,window=20)
#         upper_df = ema_trend_dataframe(df=upper_df,window=1)
#
#         main_df = df.copy()
#         main_df = exponential_moving_average_dataframe(df=main_df, window=20)
#         main_df = modified_bollinger_band_dataframe(df=main_df,column='close',window=20)
#
#         ## Buy Signal 조건 판별
#
#         scrn0 = ((upper_df['ema_trend']!=-1)&(upper_df['macd_histogram_trend']>=0)).iloc[-1]
#         if ~scrn0:
#             return pd.DataFrame(columns=list(main_df.columns)), main_df
#
#         cond0 = (main_df['close'] >= (2*main_df['ema'] + main_df['lower_bollinger']) / 3) & (main_df['close'] < (2 * main_df['ema'] + main_df['upper_bollinger']) / 3)
#         cond1 = main_df['volume'] <= 10
#         signal_cond = cond0 & cond1
#
#         ## Buy Signal 확인 및 결과 리턴
#
#         bsdf = main_df[signal_cond]
#
#         if (len(bsdf)!=0):
#             if (main_df['datetime'].iloc[-1] == bsdf['datetime'].iloc[-1]):
#                 return bsdf.iloc[-1:].reset_index(drop=True), main_df
#
#         return pd.DataFrame(columns=list(main_df.columns)), main_df
#
#
#     def determine_buy_order_plan_and_info(self,bsdf, df):
#
#         ## Columns
#         add_header_columns = ['market','coin','datetime']
#         add_buy_info_columns = ['trd_id', 'pattern', 'timeframe', 'bs_dt_i', 'bs_dt_r', 'bsg_latency']
#         add_buy_plan_columns = ['planned_ent_price','planned_lc_price','planned_gc_price','planned_units','initial_risk','ent_band_upper','ent_band_lower','ent_band_width']
#
#         add_rest_columns = list(bsdf.columns)
#         for x in (add_header_columns):
#             add_rest_columns.remove(x)
#
#         total_columns = add_header_columns + add_buy_info_columns + add_buy_plan_columns + add_rest_columns
#
#         if len(bsdf)==0:
#             return pd.DataFrame(columns=total_columns)
#
#         ## Plan
#
#         # total_trd_eq = 1000000
#         fixed_risk = 20000
#
#         bsdf['planned_ent_price'] = df.iloc[-1]['close']
#         bsdf['planned_lc_price'] = bsdf.iloc[-1]['planned_ent_price'] - (df[['open','close']].min(axis=1)-df['low']).rolling(center=False,window=250).max().iloc[-1]
#         bsdf['planned_gc_price'] = math.ceil(df.iloc[-1]['close']*1.1*100)/100
#         bsdf['planned_units'] = math.ceil(1/10*(fixed_risk / (bsdf.iloc[-1]['planned_ent_price'] - bsdf.iloc[-1]['planned_lc_price'])*100))/100
#         bsdf['initial_risk'] = math.ceil(((bsdf['planned_ent_price'] - bsdf['planned_lc_price'])*bsdf['planned_units']).iloc[-1]*100)/100
#         bsdf['ent_band_upper'] = math.ceil(df.iloc[-1]['upper_bollinger']*100)/100
#         bsdf['ent_band_lower'] = math.ceil(df.iloc[-1]['lower_bollinger']*100)/100
#         bsdf['ent_band_width'] = math.ceil(df.iloc[-1]['avg_width_bollinger']*100)/100
#
#         ## Info
#
#         time_now = datetime.now()
#         time_now_str = time_now.strftime('%Y-%m-%dT%H:%M:%S.%f')
#         last_df_dt = datetime.strptime(df.iloc[-1]['datetime'],'%Y-%m-%dT%H:%M:%S')
#         df_timeframe_seconds = (last_df_dt - datetime.strptime(df.iloc[-2]['datetime'],'%Y-%m-%dT%H:%M:%S')).total_seconds()
#         bs_time_i_dt = last_df_dt + timedelta(seconds=df_timeframe_seconds)
#         bs_time_i_str = bs_time_i_dt.strftime('%Y-%m-%dT%H:%M:%S.%f')
#
#         bsdf['pattern'] = 'ptest'
#         bsdf['trd_id'] = '{}{}'.format(bsdf.iloc[-1]['pattern'],time_now_str.replace('-','').replace(':','').replace('.','')[:-3])
#         bsdf['timeframe'] = df_timeframe_seconds
#         bsdf['bs_dt_i'] = bs_time_i_str
#         bsdf['bs_dt_r'] = time_now_str
#         bsdf['bsg_latency'] = (time_now - bs_time_i_dt).total_seconds()
#
#         return bsdf[total_columns]
#
#
#     ## sell signal generation
#
#     def sell_signal_generation_main(self, bsdf, df):
#         planned_ext_info, ssdf, df = self.sell_pattern_recognition(bsdf=bsdf,df=df)
#         ssdf = self.determine_sell_order_plan_and_info(planned_ext_info=planned_ext_info,ssdf=ssdf,df=df)
#         return ssdf
#
#     @staticmethod
#     def sell_pattern_recognition(bsdf,df):
#
#         ## 사용지표구성
#
#         bsdf_for_sell = bsdf.copy()
#         bsdf_for_sell_row = bsdf_for_sell.iloc[0]
#
#         upper_df = get_upper_timeframe_ohlcv_df(df=df,time_interval='30T')
#         upper_df = macd_hist_dataframe(df=upper_df)
#         upper_df = exponential_moving_average_dataframe(df=upper_df,window=20)
#         upper_df = ema_trend_dataframe(df=upper_df,window=1)
#
#         main_df = df.copy()
#         main_df = exponential_moving_average_dataframe(df=main_df, window=20)
#         main_df = modified_bollinger_band_dataframe(df=main_df,column='close',window=20)
#
#         ## Sell Signal 조건 판별
#
#         # 스크리닝 조건
#
#         scrn_cond0 = ((upper_df['ema_trend']==-1)|(upper_df['macd_histogram_trend']<0)).iloc[-1]
#         scrn_cond_total = ~scrn_cond0
#         if scrn_cond_total:
#             ext_type = 'sc'
#             planned_ext_price = main_df.iloc[-1]['close']
#             planned_ext_info = (ext_type,planned_ext_price)
#             return planned_ext_info, bsdf_for_sell, main_df
#
#         # 타임컷 조건
#
#         tc_cond0 = (datetime.now() - datetime.strptime(bsdf_for_sell_row['bs_dt_r'],'%Y-%m-%dT%H:%M:%S.%f')).total_seconds() >= bsdf_for_sell_row['timeframe']*72
#         tc_cond_total = tc_cond0
#         if tc_cond_total:
#             ext_type = 'tc'
#             planned_ext_price = main_df.iloc[-1]['close']
#             planned_ext_info = (ext_type,planned_ext_price)
#             return planned_ext_info, bsdf_for_sell, main_df
#
#         # 익절 조건
#         gc_cond0 = main_df['close'] >= ((main_df['ema'] + 7*main_df['upper_bollinger']) / 8)
#         gc_cond_total = gc_cond0
#         if gc_cond_total.iloc[-1]:
#             ext_type = 'gc'
#             planned_ext_price = main_df.iloc[-1]['close']
#             planned_ext_info = (ext_type,planned_ext_price)
#             return planned_ext_info, bsdf_for_sell, main_df
#
#         # 손절 조건
#         lc_cond0 = main_df['close'] <= ((main_df['ema'] + main_df['lower_bollinger']) / 2)
#         lc_cond1 = main_df['close'] <= bsdf_for_sell_row['ent_price']*0.95
#         lc_cond_total = lc_cond0 | lc_cond1
#         if lc_cond_total.iloc[-1]:
#             ext_type = 'lc'
#             planned_ext_price = bsdf_for_sell.iloc[-1]['planned_lc_price']
#             planned_ext_info = (ext_type,planned_ext_price)
#             return planned_ext_info, bsdf_for_sell, main_df
#
#
#         # 아무 조건에도 속하지 않을 경우
#         ext_type = None
#         planned_ext_price = np.nan
#         planned_ext_info = (ext_type, planned_ext_price)
#         return planned_ext_info, pd.DataFrame(columns=list(bsdf.columns)), main_df
#
#
#
#     def determine_sell_order_plan_and_info(self,planned_ext_info, ssdf, df):
#
#         ## Columns
#         add_header_columns = ['market', 'coin', 'datetime']
#         add_buy_info_columns = ['trd_id', 'pattern', 'timeframe', 'bs_dt_i', 'bs_dt_r', 'bsg_latency']
#         add_buy_plan_columns = ['planned_ent_price','planned_lc_price','planned_gc_price','planned_units','initial_risk','ent_band_upper','ent_band_lower','ent_band_width']
#         add_buy_result_columns = ['ent_dt','ent_order_num','ent_price','units','ent_score']
#         add_sell_plan_columns = ['ss_dt_i','ss_dt_r','ssg_latency','ext_type','planned_ext_price','ext_band_upper','ext_band_lower','ext_band_width']
#
#
#         add_rest_columns = list(ssdf.columns)
#         for x in (add_header_columns + add_buy_info_columns + add_buy_plan_columns + add_buy_result_columns):
#             add_rest_columns.remove(x)
#
#         total_columns = add_header_columns + add_buy_info_columns + add_buy_plan_columns + add_buy_result_columns + add_sell_plan_columns + add_rest_columns
#
#         if len(ssdf) == 0:
#             return pd.DataFrame(columns=total_columns)
#
#         ## Plan
#
#         ss_plan_df = ssdf.copy()
#         ss_plan_df['ext_type'] = planned_ext_info[0]
#         ss_plan_df['planned_ext_price'] = planned_ext_info[1]
#         ss_plan_df['ext_band_upper'] = math.ceil(df.iloc[-1]['upper_bollinger']*100)/100
#         ss_plan_df['ext_band_lower'] = math.ceil(df.iloc[-1]['lower_bollinger']*100)/100
#         ss_plan_df['ext_band_width'] = math.ceil(df.iloc[-1]['avg_width_bollinger']*100)/100
#
#
#         ## Info
#
#         time_now = datetime.now()
#         time_now_str = time_now.strftime('%Y-%m-%dT%H:%M:%S.%f')
#         last_df_dt = datetime.strptime(df.iloc[-1]['datetime'], '%Y-%m-%dT%H:%M:%S')
#         df_timeframe_seconds = (last_df_dt - datetime.strptime(df.iloc[-2]['datetime'], '%Y-%m-%dT%H:%M:%S')).total_seconds()
#         ss_time_i_dt = last_df_dt + timedelta(seconds=df_timeframe_seconds)
#         ss_time_i_str = ss_time_i_dt.strftime('%Y-%m-%dT%H:%M:%S.%f')
#
#
#         ss_plan_df['ss_dt_i'] = ss_time_i_str
#         ss_plan_df['ss_dt_r'] = time_now_str
#         ss_plan_df['ssg_latency'] = (time_now - ss_time_i_dt).total_seconds()
#
#         return ss_plan_df[total_columns]
#
