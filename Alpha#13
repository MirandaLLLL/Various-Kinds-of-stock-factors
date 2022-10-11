import pandas as pd
import numpy as np
import cx_Oracle
import alphalens as al

stock_data= pd.read_pickle('D:/stock_data.pkl')



data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','CLOSE_PRICE','VWAP','TURNOVER_VOL']]


data_use['volume']=data_use['TURNOVER_VOL']/data_use['VWAP']

data_use=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index()

rank_volume_1=data_use.sort_values(by=['TICKER_SYMBOL','TRADE_DATE']).set_index(['TICKER_SYMBOL','TRADE_DATE'])
rank_volume_2=rank_volume_1['volume'].unstack()
rank_volume_3=(rank_volume_2.rank()-1)/len(rank_volume_2)

rank_close_1=data_use.sort_values(by=['TICKER_SYMBOL','TRADE_DATE']).set_index(['TICKER_SYMBOL','TRADE_DATE'])
rank_close_2=rank_close_1['CLOSE_PRICE'].unstack()
rank_close_3=(rank_close_2.rank()-1)/len(rank_close_2)

rank_volume_table=pd.merge(rank_volume_3.stack().reset_index(),data_use,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer').sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

rank_volume_table=rank_volume_table.rename(columns={0:'rank_volume'})
rank_table=pd.merge(rank_volume_table,rank_close_3.stack().reset_index(),on=['TICKER_SYMBOL','TRADE_DATE'],how='outer').sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
rank_table=rank_table.rename(columns={0:'rank_close'})

rank_cov=rank_table[["rank_volume","rank_close"]]

rank_cov_1=rank_cov.shift().rolling(5).cov()

rank_cov_2=rank_cov_1.reset_index()

rank_cov_3=rank_cov_2.drop_duplicates(subset={'level_0'},keep='last').drop(columns={'level_1','rank_close'})

rank_cov_3=rank_cov_3.set_index(["level_0","rank_volume"]).reset_index()
rank_cov_3['index']=rank_cov_3['level_0'].rank()

rank_table['index']=rank_cov_3['level_0'].rank()

cov_table=pd.merge(rank_table,rank_cov_3,on=['index'])
cov_table=cov_table.rename(columns={'rank_volume_x':'rank_volume','rank_volume_y':'cov'}).drop(columns={'level_0','index'})
condition=(cov_table.TICKER_SYMBOL==cov_table.shift(5).TICKER_SYMBOL)
cov_table['cov']=cov_table['cov'].where(condition,np.nan)

rank_final_1=cov_table.sort_values(by=['TICKER_SYMBOL','TRADE_DATE']).set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_final_2=rank_final_1['cov'].unstack()
rank_final_3=(-1)*(rank_final_2.rank()-1)/(len(rank_final_2)-1)

rank_final_table=pd.merge(rank_final_3.stack().reset_index(),cov_table,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer')
final=rank_final_table.rename(columns={0:'alpha#13'}).set_index(["TRADE_DATE","TICKER_SYMBOL"])


factor_init = final["alpha#13"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)

al.tears.create_returns_tear_sheet(factor)
ic=al.performance.factor_information_coefficient(factor)
