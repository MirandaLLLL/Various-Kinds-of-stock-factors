import numpy as np
import pandas as pd
import alphalens as al

stock_data=pd.read_pickle('D:/stock_data.pkl')

data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','CLOSE_PRICE','VWAP','TURNOVER_VOL']]
data_use=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index().sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

data_use['volume']=data_use['TURNOVER_VOL']/data_use['VWAP']

rank_volume_1=data_use.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_volume_2=rank_volume_1['VWAP'].unstack()
rank_volume_3=(rank_volume_2.rank()-1)/(len(rank_volume_2)-1)
rank_vwap_2=rank_volume_1['TURNOVER_VOL'].unstack()
rank_vwap_3=(rank_vwap_2.rank()-1)/(len(rank_volume_2)-1)
rank_table_1=pd.merge(rank_volume_3.stack().reset_index(),data_use,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'rank_volume'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
rank_table_2=pd.merge(rank_vwap_3.stack().reset_index(),rank_table_1,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'rank_vwap'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

corr_1=rank_table_2[['rank_vwap','rank_volume']]
corr_2=corr_1.shift().rolling(6).corr()
corr_3=corr_2.reset_index().drop_duplicates(subset={'level_0'},keep='last').set_index(["level_0","level_1"]).reset_index()
corr_4=corr_3.drop(columns={'level_1','rank_volume'}).rename(columns={'rank_vwap':'corr'})
rank_table_2=rank_table_2.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index()
rank_table_2['level_0']=corr_4['level_0']

corr_table=pd.merge(corr_4,rank_table_2,on=['level_0']).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
corr_table=corr_table.drop(columns={'level_0'})
condition=corr_table.TICKER_SYMBOL==corr_table.shift(7).TICKER_SYMBOL
corr_table['corr']=corr_table['corr'].where(condition,np.nan)

mean_1=corr_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])
mean_2=mean_1['corr'].unstack()
mean_3=mean_2.rolling(3).mean()
mean_table=pd.merge(mean_3.stack().reset_index(),corr_table,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'mean'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

rank_m_1=mean_table.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_m_2=rank_m_1['mean'].unstack()
rank_m_3=(rank_m_2.rank()-1)/(len(rank_m_2)-1)
rank_m_table=pd.merge(rank_m_3.stack().reset_index(),mean_table,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'rank_m'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
rank_m_table['alpha#27']=rank_m_table['rank_m']*(-1)

final=rank_m_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])
factor_init = final["alpha#27"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=1)

al.tears.create_returns_tear_sheet(factor)

ic=al.performance.factor_information_coefficient(factor)
