import numpy as np
import pandas as pd
import alphalens as al

stock_data=pd.read_pickle('D:/stock_data.pkl')

data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','CLOSE_PRICE','VWAP','TURNOVER_VOL','HIGHEST_PRICE']]
data_use=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index().sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
data_use['volume']=data_use['TURNOVER_VOL']/data_use['VWAP']

def rank(array):
    s = pd.Series(array)
    return s.rank()[len(s)-1]

ts_rank_high_1=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
ts_rank_high_2=ts_rank_high_1['HIGHEST_PRICE'].unstack()
ts_rank_high_3=(ts_rank_high_2.rolling(5).apply(rank)-1)/4

ts_rank_volume_1=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
ts_rank_volume_2=ts_rank_volume_1['volume'].unstack()
ts_rank_volume_3=(ts_rank_volume_2.rolling(5).apply(rank)-1)/4

ts_table_1=pd.merge(ts_rank_high_3.stack().reset_index(),data_use,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'ts_rank_high'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
ts_table_2=pd.merge(ts_rank_volume_3.stack().reset_index(),ts_table_1,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'ts_rank_volume'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
ts_table_2=ts_table_2.set_index(["TRADE_DATE","TICKER_SYMBOL"])
corr_1=ts_table_2[['ts_rank_high','ts_rank_volume']]
corr_2=corr_1.shift().rolling(3).corr()
corr_3=corr_2.reset_index().drop_duplicates(subset={'level_0'},keep='last').set_index(["level_0","level_1"]).reset_index()
corr_4=corr_3.drop(columns={'level_1','ts_rank_volume'}).rename(columns={'ts_rank_high':'corr'})
ts_table_2['level_0']=corr_4['level_0']
corr_table=pd.merge(ts_table_2,corr_4,on=['level_0']).drop(columns={'level_0'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
condition=corr_table.TICKER_SYMBOL==corr_table.shift(4).TICKER_SYMBOL
corr_table['corr']=corr_table['corr'].where(condition,np.nan)
ts_max_1=corr_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])
ts_max_2=ts_max_1['corr'].unstack()
ts_max_3=ts_max_2.shift().rolling(3).corr()
ts_max_table=pd.merge(ts_max_3.stack().reset_index(),corr_table,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'ts_max'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

ts_max_table['alpha#26']=ts_max_table['ts_max']*(-1)
final=ts_max_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])

factor_init = final["alpha#26"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=1)

al.tears.create_returns_tear_sheet(factor)

ic=al.performance.factor_information_coefficient(factor)
