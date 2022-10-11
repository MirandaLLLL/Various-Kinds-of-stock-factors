import pandas as pd
import cx_Oracle
import calendar
import alphalens as al
import numpy as np

stock_data=pd.read_pickle('D:/stock_data.pkl')

data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','OPEN_PRICE','CLOSE_PRICE']].set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index()

returns=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
returns_un=returns['CLOSE_PRICE'].unstack()
returns_1=returns_un.diff()
returns_2=returns_un.diff()/returns_un.shift()

return_table=pd.merge(data_use,returns_1.stack().reset_index(),on=['TICKER_SYMBOL','TRADE_DATE'],how='outer')

return_table=return_table.rename(columns={0:'returns'})

return_table_final=pd.merge(return_table,returns_2.stack().reset_index(),on=['TICKER_SYMBOL','TRADE_DATE'],how='outer')

return_table_final=return_table_final.rename(columns={0:'return_rate'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

return1_sum_1=return_table_final.set_index(["TRADE_DATE","TICKER_SYMBOL"])
return1_sum_2=return1_sum_1['returns'].unstack()
return1_sum_3=return1_sum_2.shift().rolling(5).sum()
return1_sum_4=return1_sum_3.stack().reset_index()
table=pd.merge(return1_sum_4,return_table_final,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer')

table=table.rename(columns={0:'return1_sum'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

return2_sum_1=return_table_final.set_index(["TRADE_DATE","TICKER_SYMBOL"])
return2_sum_2=return2_sum_1['return_rate'].unstack()
return2_sum_3=return2_sum_2.shift().rolling(5).sum()
return2_sum_4=return2_sum_3.stack().reset_index()
table_2=pd.merge(return2_sum_4,table,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer')
table_2=table_2.rename(columns={0:'return2_sum'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

open_sum_1=return_table_final.set_index(["TRADE_DATE","TICKER_SYMBOL"])
open_sum_2=open_sum_1['OPEN_PRICE'].unstack()
open_sum_3=open_sum_2.shift().rolling(5).sum()
open_sum_4=open_sum_3.stack().reset_index()
table_3=pd.merge(open_sum_4,table_2,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer')
table_3=table_3.rename(columns={0:'open_sum'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

table_3['open*r1']=table_3['open_sum']*table_3['return1_sum']
table_3['open*r2']=table_3['open_sum']*table_3['return2_sum']

delay1_1=table_3.set_index(["TRADE_DATE","TICKER_SYMBOL"])
delay1_2=delay1_1['return1_sum'].unstack()
delay1_3=delay1_2.shift(10)
delay1_4=delay1_3.stack().reset_index()
delay1_table=pd.merge(delay1_4,table_3,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer')
delay1_table=delay1_table.rename(columns={0:'delay1'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

delay2_1=table_3.set_index(["TRADE_DATE","TICKER_SYMBOL"])
delay2_2=delay2_1['return2_sum'].unstack()
delay2_3=delay2_2.shift(10)
delay2_4=delay2_3.stack().reset_index()
delay2_table=pd.merge(delay2_4,delay1_table,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer')
delay2_table=delay2_table.rename(columns={0:'delay2'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

delay2_table['x1']=delay2_table['open_sum']*delay2_table['return1_sum']-delay2_table['delay1']
delay2_table['x2']=delay2_table['open_sum']*delay2_table['return2_sum']-delay2_table['delay2']

rank1_1=delay2_table.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank1_2=rank1_1['x1'].unstack()
rank1_3=(rank1_2.rank()-1)/len(rank1_2)
rank1_4=rank1_3.stack().reset_index()
rank1_table=pd.merge(rank1_4,delay2_table,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer')

rank2_1=delay2_table.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank2_2=rank2_1['x2'].unstack()
rank2_3=(rank2_2.rank()-1)/len(rank2_2)
rank2_4=rank2_3.stack().reset_index()
rank2_table=pd.merge(rank2_4,rank1_table,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer')

rank2_table=rank2_table.rename(columns={'0_x':'alpha#8_rate','0_y':'alpha#8_returns'})

final=rank2_table.copy()
final['alpha#8_rate']=(-1)*final['alpha#8_rate']
final['alpha#8_returns']=(-1)*final['alpha#8_returns']

factor_init = final["alpha#8_rate"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)

al.tears.create_returns_tear_sheet(factor)
ic=al.performance.factor_information_coefficient(factor)

factor_init = final["alpha#8_returns"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)

al.tears.create_returns_tear_sheet(factor)
ic=al.performance.factor_information_coefficient(factor)
