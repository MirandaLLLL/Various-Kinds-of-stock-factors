import pandas as pd
import numpy as np
import cx_Oracle
import alphalens as al

stock_data= pd.read_pickle('D:/stock_data.pkl')



data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','CLOSE_PRICE','VWAP','TURNOVER_VOL','HIGHEST_PRICE']]

data_use=data_use.set_index(["TICKER_SYMBOL","TRADE_DATE"]).reset_index()
data_use['volume']=data_use['TURNOVER_VOL']/data_use['VWAP']
rank_volume_1=data_use.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_volume_2=rank_volume_1['volume'].unstack()
rank_volume_3=(rank_volume_2.rank()-1)/(len(rank_volume_2)-1)
rank_volume_4=rank_volume_3.stack().reset_index()

rank_high_1=data_use.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_high_2=rank_high_1['HIGHEST_PRICE'].unstack()
rank_high_3=(rank_high_2.rank()-1)/(len(rank_high_2)-1)
rank_high_4=rank_high_3.stack().reset_index()

rank_table_1=pd.merge(rank_volume_4,data_use,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer').rename(columns={0:'rank_volume'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

rank_table_2=pd.merge(rank_high_4,rank_table_1,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer').rename(columns={0:'rank_high'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

corr_1=rank_table_2[['rank_volume','rank_high']]
corr_2=corr_1.rolling(3).corr()
corr_3=corr_2.reset_index()

corr_3=corr_3.drop_duplicates(subset={'level_0'},keep='last')
corr_4=corr_3.set_index(["level_0","level_1"]).reset_index().drop(columns={'rank_high','level_1'}).rename(columns={'rank_volume':'corr'})
corr_4['index']=corr_4['level_0'].rank()
rank_table_2['index']=corr_4['level_0'].rank()
corr_table=pd.merge(rank_table_2,corr_4,on=['index']).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

rank_corr_1=corr_table.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_corr_2=rank_corr_1['corr'].unstack()
rank_corr_3=(rank_corr_2.rank()-1)/(len(rank_corr_2)-1)
rank_corr_table=pd.merge(corr_table,rank_corr_3.stack().reset_index(),on=['TICKER_SYMBOL','TRADE_DATE'],how='outer').sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

rank_corr_table=rank_corr_table.rename(columns={0:'rank_corr'})
sum_1=rank_corr_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])
sum_2=sum_1['rank_corr'].unstack()
sum_3=(sum_2.shift().rolling(3).sum())*(-1)

final=pd.merge(sum_3.stack().reset_index(),rank_corr_table,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer').rename(columns={0:'alpha#15'}).set_index(["TRADE_DATE","TICKER_SYMBOL"])

factor_init = final["alpha#15"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)

al.tears.create_returns_tear_sheet(factor)
ic=al.performance.factor_information_coefficient(factor)
