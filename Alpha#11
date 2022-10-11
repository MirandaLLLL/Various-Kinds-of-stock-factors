import pandas as pd
import numpy as np
import alphalens as al

stock_data= pd.read_pickle('D:/stock_data.pkl')

data_use=stock_data[["TICKER_SYMBOL","TRADE_DATE","CLOSE_PRICE","VWAP","TURNOVER_VOL"]]

data_use['volume']=data_use['TURNOVER_VOL']/data_use['VWAP']

data_use=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index().sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

data_use['vwap-close']=data_use['VWAP']-data_use['CLOSE_PRICE']

ts_min_1=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
ts_min_2=ts_min_1['vwap-close'].unstack()
ts_min_3=ts_min_2.shift().rolling(3).min()
ts_max=ts_min_2.shift().rolling(3).max()


ts_table1=pd.merge(data_use,ts_min_3.stack().reset_index(),on=['TRADE_DATE','TICKER_SYMBOL']).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])


ts_table1=ts_table1.rename(columns={0:'ts_min'})

ts_table2=pd.merge(ts_table1,ts_max.stack().reset_index(),on=['TRADE_DATE','TICKER_SYMBOL'],how='outer')


ts_table2=ts_table2.rename(columns={0:'ts_max'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
rank_max_1=ts_table2.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_max_2=rank_max_1['ts_max'].unstack()
rank_min_2=rank_max_1['ts_min'].unstack()
rank_max_3=(rank_max_2.rank()-1)/len(rank_max_2)
rank_min_3=(rank_min_2.rank()-1)/len(rank_min_2)
rank_max_table=pd.merge(rank_max_3.stack().reset_index(),ts_table2,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer').sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])


rank_max_table=rank_max_table.rename(columns={0:'rank_max'})
rank_table=pd.merge(rank_max_table,rank_min_3.stack().reset_index(),on=['TICKER_SYMBOL','TRADE_DATE'],how='outer').sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
rank_table=rank_table.rename(columns={0:'rank_min'})

rank_table['rank_min+rank_max']=rank_table['rank_min']+rank_table['rank_max']
delta_1=rank_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])
delta_2=delta_1['volume'].unstack()
delta_3=delta_2-delta_2.shift(3)
delta_table=pd.merge(delta_3.stack().reset_index(),rank_table,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer').sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

delta_table=delta_table.rename(columns={0:'delta'})

rank_delta_1=delta_table.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_delta_2=rank_delta_1['delta'].unstack()
rank_delta_3=(rank_delta_2.rank()-1)/len(rank_delta_2)

rank_delta_table=pd.merge(rank_delta_3.stack().reset_index(),delta_table,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer').sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])


rank_delta_table=rank_delta_table.rename(columns={0:'rank_delta'})
rank_delta_table['alpha#11']=rank_delta_table['rank_delta']*rank_delta_table['rank_min+rank_max']

final=rank_delta_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])

factor_init = final["alpha#11"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)

al.tears.create_returns_tear_sheet(factor)

ic=al.performance.factor_information_coefficient(factor)
