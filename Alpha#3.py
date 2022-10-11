import pandas as pd
import numpy as np
import cx_Oracle
import alphalens as al

stock_data= pd.read_pickle('D:/stock_data.pkl')

data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','OPEN_PRICE','CLOSE_PRICE','VWAP','TURNOVER_VOL']]


data_use=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index()
data_use['volume']=data_use['TURNOVER_VOL']/data_use['VWAP']
data_use_copy=data_use.copy()
data_use=data_use.sort_values(['TICKER_SYMBOL','TRADE_DATE']).set_index(["TRADE_DATE","TICKER_SYMBOL"])
volume_rank=data_use['volume'].unstack()
open_rank=data_use['OPEN_PRICE'].unstack()

volume_rank_up=(volume_rank.rank()-1)/(len(volume_rank)-1)

open_rank_up=(open_rank.rank()-1)/(len(open_rank)-1)

V_data=volume_rank_up.stack().reset_index()
O_data=open_rank_up.stack().reset_index()

V_table=pd.merge(V_data,data_use_copy)

V_table=V_table.rename(columns={0:'v_rank'})
VO_table=pd.merge(V_table,O_data)

VO_table=VO_table.rename(columns={0:'o_rank'})

VO_corr=VO_table[['v_rank','o_rank']]

VO_corr=VO_corr.shift().rolling(10).corr()

VO_corr=VO_corr.reset_index()


VO_corr=VO_corr.drop_duplicates(subset={'level_0'},keep='last')
VO_corr=VO_corr.drop(columns={'o_rank','level_1'})

VO_corr=VO_corr.set_index(["level_0","v_rank"]).reset_index()


VO_corr=VO_corr.rename(columns={'v_rank':'corr'})
VO_corr['index']=VO_corr['level_0'].rank()
VO_table['index']=VO_corr['level_0'].rank()

VO_merge=pd.merge(VO_corr,VO_table)


VO_merge=VO_merge.drop(columns={'level_0','index'})

final=VO_merge.copy()
final=final.rename(columns={'corr':'alpha#3'})

final=final.reset_index()
final['alpha#3']=(-1)*final['alpha#3']

final=final.set_index(["TRADE_DATE","TICKER_SYMBOL"])

factor_init = final["alpha#3"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)

al.tears.create_returns_tear_sheet(factor)
