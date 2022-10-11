import pandas as pd
import numpy as np
import cx_Oracle
import alphalens as al

stock_data= pd.read_pickle('D:/stock_data.pkl')

data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','OPEN_PRICE','CLOSE_PRICE','VWAP']]

data_use['close-vwap']=data_use['CLOSE_PRICE']-data_use['VWAP']
rank_x1=data_use.set_index(["TICKER_SYMBOL","TRADE_DATE"])

rank_x1_un=rank_x1['close-vwap'].unstack()

rank_1_c=(-1)*abs(rank_x1_un.rank()-1)/len(rank_x1_un-1)

rank_1_re=rank_1_c.stack().reset_index()
rank1_table=pd.merge(rank_1_re,data_use,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer')

rank1_table=rank1_table.rename(columns={0:'rank_x1'})

sum_vwap=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
sum_vwap_un=sum_vwap['VWAP'].unstack()
sum_vwap_un_copy=sum_vwap_un.copy()
sum_vwap_un=sum_vwap_un.shift().rolling(10).sum()/10


sum_vwap_re=sum_vwap_un.stack().reset_index()
sum_vwap_table=pd.merge(sum_vwap_re,rank1_table,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer')

sum_vwap_table=sum_vwap_table.rename(columns={0:'sum_vwap'})

sum_vwap_table['open-sum']=sum_vwap_table['OPEN_PRICE']-sum_vwap_table['sum_vwap']
rank_y1=sum_vwap_table.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_y1_un=rank_y1['open-sum'].unstack()

rank_y1_un_copy=rank_y1_un.copy()
rank_y_c=(rank_y1_un.rank()-1)/(len(rank_y1_un)-1)

rank_y_re=rank_y_c.stack().reset_index()
rank_y_table=pd.merge(rank_y_re,sum_vwap_table,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer')
rank_y_table=rank_y_table.rename(columns={0:'y1'})
final=rank_y_table.copy()
final['alpha#5']=final['y1']*final['rank_x1']

final=final.set_index(["TRADE_DATE","TICKER_SYMBOL"])

factor_init = final["alpha#5"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)
al.tears.create_returns_tear_sheet(factor)
