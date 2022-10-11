import pandas as pd
import numpy as np
import cx_Oracle
import alphalens as al

stock_data= pd.read_pickle('D:/stock_data.pkl')



data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','CLOSE_PRICE','VWAP','TURNOVER_VOL','OPEN_PRICE']]


data_use['volume']=data_use['TURNOVER_VOL']/data_use['VWAP']

data_use=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index()

data_use=data_use.sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
cov_1=data_use[['OPEN_PRICE','volume']]

cov_2=cov_1.rolling(10).corr()

cov_3=cov_2.reset_index()

cov_4=cov_3.drop_duplicates(subset={'level_0'},keep='last')

cov_4=cov_4.set_index(["level_0","level_1"]).reset_index()

cov_5=cov_4.drop(columns={'volume','level_1'})
cov_5['index']=cov_5['level_0'].rank()
data_use['index']=cov_5['level_0'].rank()
cov_table=pd.merge(cov_5,data_use,on=['index'])

cov_table=cov_table.drop(columns={'index','level_0'}).rename(columns={'OPEN_PRICE_x':'corr','OPEN_PRICE_y':'OPEN_PRICE'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

returns_1=cov_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])
returns_2=returns_1['CLOSE_PRICE'].unstack()
returns_3=returns_2-returns_2.shift()
return_rate=(returns_2-returns_2.shift())/returns_2.shift()
returns_4=returns_3.stack().reset_index()
returns_table=pd.merge(returns_4,cov_table,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer').sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

returns_table=returns_table.rename(columns={0:'returns'})
return_rate_table=pd.merge(returns_table,return_rate.stack().reset_index(),on=['TICKER_SYMBOL','TRADE_DATE'],how='outer')

return_rate_table=return_rate_table.rename(columns={0:'return_rate'}).sort_values(["TICKER_SYMBOL","TRADE_DATE"])
delta_return_1=return_rate_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])
delta_return_2=delta_return_1['returns'].unstack()
delta_return_3=delta_return_2-delta_return_2.shift(3)
delta_return_rate_1=return_rate_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])
delta_return_rate_2=delta_return_rate_1['return_rate'].unstack()
delta_return_rate_3=delta_return_rate_2-delta_return_rate_2.shift(3)
delta_table_1=pd.merge(delta_return_3.stack().reset_index(),return_rate_table,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer').rename(columns={0:'delta_returns'})
delta_table_2=pd.merge(delta_table_1,delta_return_rate_3.stack().reset_index(),on=['TICKER_SYMBOL','TRADE_DATE'],how='outer').rename(columns={0:'delta_return_rate'})

delta_table_2=delta_table_2.sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

rank_return_1=delta_table_2.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_return_2=rank_return_1['delta_returns'].unstack()
rank_return_3=(-1)*(rank_return_2.rank()-1)/(len(rank_return_2)-1)
rank_return_rate_1=delta_table_2.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_return_rate_2=rank_return_rate_1['delta_return_rate'].unstack()
rank_return_rate_3=(-1)*(rank_return_rate_2.rank()-1)/(len(rank_return_rate_2)-1)

rank_table_1=pd.merge(rank_return_3.stack().reset_index(),delta_table_2,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer').rename(columns={0:'rank_return'})
rank_table_2=pd.merge(rank_return_rate_3.stack().reset_index(),rank_table_1,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer').rename(columns={0:'rank_return_rate'})

rank_table_2=rank_table_2.sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

rank_table_2['alpha#14_returns']=rank_table_2['corr']*rank_table_2['rank_return']
rank_table_2['alpha#14_return_rate']=rank_table_2['corr']*rank_table_2['rank_return_rate']

final=rank_table_2.set_index(["TRADE_DATE","TICKER_SYMBOL"])

factor_init = final["alpha#14_return_rate"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)

al.tears.create_returns_tear_sheet(factor)
ic=al.performance.factor_information_coefficient(factor)
