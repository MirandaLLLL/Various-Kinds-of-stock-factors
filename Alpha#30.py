import numpy as np
import pandas as pd
import alphalens as al

stock_data=pd.read_pickle('D:/stock_data.pkl')

data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','CLOSE_PRICE','VWAP','TURNOVER_VOL']]
data_use=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index().sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
data_use['volume']=data_use['TURNOVER_VOL']/data_use['VWAP']
close_set=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
close_set_1=close_set['CLOSE_PRICE'].unstack()
delta_1=np.sign(close_set_1-close_set_1.shift())
delta_2=np.sign(delta_1.shift())
delta_3=np.sign(delta_2.shift())
sign_table_1=pd.merge(delta_1.stack().reset_index(),data_use,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'sign_1'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
sign_table_2=pd.merge(delta_2.stack().reset_index(),sign_table_1,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'sign_2'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
sign_table_3=pd.merge(delta_3.stack().reset_index(),sign_table_2,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'sign_3'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

sum_volume_1=close_set['volume'].unstack()
sum_volume_5=sum_volume_1.shift().rolling(5).sum()
sum_volume_20=sum_volume_1.shift().rolling(20).sum()
sum_table_1=pd.merge(sum_volume_5.stack().reset_index(),sign_table_3,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'sum_5'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

sum_table_2=pd.merge(sum_volume_20.stack().reset_index(),sum_table_1,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'sum_20'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

sum_table_2['x1']=sum_table_2['sign_1']*3+sum_table_2['sign_2']*1+sum_table_2['sign_3']*1

rank_1=sum_table_2.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_2=rank_1['x1'].unstack()
rank_3=1-((rank_2.rank()-1)/(len(rank_2)-1))
rank_table=pd.merge(rank_3.stack().reset_index(),sum_table_2,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'1-rank'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])


rank_table['alpha#30']=rank_table['1-rank']*rank_table['sum_20']/rank_table['sum_5']
final=rank_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])

factor_init = final["alpha#30"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=1)

al.tears.create_returns_tear_sheet(factor)

ic=al.performance.factor_information_coefficient(factor)
