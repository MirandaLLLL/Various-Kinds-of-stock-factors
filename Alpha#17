import pandas as pd
import numpy as np
import cx_Oracle
import alphalens as al

stock_data= pd.read_pickle('D:/stock_data.pkl')



data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','CLOSE_PRICE','VWAP','TURNOVER_VOL']]

data_use=data_use.set_index(["TICKER_SYMBOL","TRADE_DATE"]).reset_index()
data_use['volume']=data_use['TURNOVER_VOL']/data_use['VWAP']


data_use=data_use.sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
delta_1=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
delta_2=delta_1['CLOSE_PRICE'].unstack()
delta_3=delta_2-delta_2.shift()
delta_4=delta_3-delta_3.shift()
delta_5=delta_4.stack().reset_index()
rank_del_1=delta_5.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_del_2=rank_del_1[0].unstack()
rank_del_3=(rank_del_2.rank()-1)/(len(rank_del_2)-1)
rank_del_4=rank_del_3.stack().reset_index()

rank_del_table=pd.merge(data_use,rank_del_4,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer').rename(columns={0:'rank_del'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
adv_1=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
adv_2=adv_1['volume'].unstack()
adv_3=adv_2.shift().rolling(20).mean()
adv_4=adv_3.stack().reset_index()

adv_table=pd.merge(rank_del_table,adv_4,on=['TICKER_SYMBOL','TRADE_DATE'])
adv_table=adv_table.rename(columns={0:'adv20'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
adv_table['volume/adv']=adv_table['volume']/adv_table['adv20']

ts_rank_v2adv_1=adv_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])
ts_rank_v2adv_2=ts_rank_v2adv_1['volume/adv'].unstack()
def rank(array):
    s = pd.Series(array)
    return s.rank()[len(s)-1]

ts_rank_v2adv_3=(ts_rank_v2adv_2.rolling(5).apply(rank)-1)/4

ts_rank_v2adv_table=pd.merge(ts_rank_v2adv_3.stack().reset_index(),adv_table,on=['TICKER_SYMBOL','TRADE_DATE']).rename(columns={0:'ts_rank_v2adv'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

ts_rank_close_1=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
ts_rank_close_2=ts_rank_close_1['CLOSE_PRICE'].unstack()
ts_rank_close_3=(ts_rank_close_2.rolling(10).apply(rank)-1)/9

ts_rank_close_table=pd.merge(ts_rank_close_3.stack().reset_index(),ts_rank_v2adv_table,on=['TICKER_SYMBOL','TRADE_DATE']).rename(columns={0:'ts_rank_close'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

rank_volume2adv_1=ts_rank_close_table.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_volume2adv_2=rank_volume2adv_1['ts_rank_v2adv'].unstack()
rank_volume2adv_3=(rank_volume2adv_2.rank()-1)/(len(rank_volume2adv_2)-1)
rank_close_2=rank_volume2adv_1['ts_rank_close'].unstack()
rank_close_3=(rank_close_2.rank()-1)/(len(rank_close_2)-1)

ts_v2adv_table=pd.merge(rank_volume2adv_3.stack().reset_index(),ts_rank_close_table,on=['TICKER_SYMBOL','TRADE_DATE']).rename(columns={0:'rank_v2adv'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
ts_close_table=pd.merge(rank_close_3.stack().reset_index(),ts_v2adv_table,on=['TICKER_SYMBOL','TRADE_DATE']).rename(columns={0:'rank_close'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

ts_close_table['rank_close']=ts_close_table['rank_close']*(-1)

final=ts_close_table[['TICKER_SYMBOL','TRADE_DATE','rank_close','rank_v2adv','rank_del','CLOSE_PRICE']]
final['alpha#17']=final['rank_close']*final['rank_v2adv']*final['rank_del']
final=final.set_index(["TRADE_DATE","TICKER_SYMBOL"])

factor_init = final["alpha#17"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)

al.tears.create_returns_tear_sheet(factor)
ic=al.performance.factor_information_coefficient(factor)
