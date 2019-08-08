import pandas as pd

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