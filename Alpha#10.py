import pandas as pd
import numpy as np
import alphalens as al

stock_data= pd.read_pickle('D:/stock_data.pkl')

data_use=stock_data[["TICKER_SYMBOL","TRADE_DATE","CLOSE_PRICE"]]
data_use=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index()
delta_1=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"]).unstack()
delta_2=delta_1-delta_1.shift()
delta_3=delta_2.stack().reset_index()
delta_table=pd.merge(delta_3,data_use,on=['TRADE_DATE','TICKER_SYMBOL'],how='outer').sort_values(by=['TICKER_SYMBOL','TRADE_DATE']).rename(columns={'CLOSE_PRICE_x':'delta'})

delta_table=delta_table.rename(columns={'CLOSE_PRICE_y':'CLOSE_PRICE'})

ts_min_1=delta_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])
ts_min_2=ts_min_1['delta'].unstack()
ts_min_3=ts_min_2.shift().rolling(4).min()
ts_max=ts_min_2.shift().rolling(4).max()

ts_table1=pd.merge(delta_table,ts_min_3.stack().reset_index(),on=['TRADE_DATE','TICKER_SYMBOL']).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
ts_table1=ts_table1.rename(columns={0:'ts_min','CLOSE_PRICE_y':'CLOSE_PRICE'})

ts_table2=pd.merge(ts_table1,ts_max.stack().reset_index(),on=['TRADE_DATE','TICKER_SYMBOL'],how='outer')
ts_table2=ts_table2.rename(columns={0:'ts_max'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

ts_table2['x1']=ts_table2['delta']
condition1=(ts_table2.ts_max<0)
ts_table2_copy=ts_table2.copy()

ts_table2['x1']=ts_table2['delta'].where(condition1,ts_table2['delta']*(-1))

ts_table2['x2']=ts_table2['delta']
condition2=(ts_table2.ts_min>0)
ts_table2['x2']=ts_table2['x2'].where(condition2,ts_table2['x1'])

rank_1=ts_table2.sort_values(by=['TICKER_SYMBOL','TRADE_DATE']).set_index(["TICKER_SYMBOL","TRADE_DATE"])

rank_2=rank_1['x2'].unstack()

rank_3=(rank_2.rank()-1)/len(rank_2)

rank_4=rank_3.stack().reset_index()
rank_table=pd.merge(rank_4,ts_table2,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer')

final=rank_table.rename(columns={0:'alpha#10'}).set_index(["TRADE_DATE","TICKER_SYMBOL"])



factor_init = final["alpha#10"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)

al.tears.create_returns_tear_sheet(factor)
