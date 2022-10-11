import numpy as np
import pandas as pd
import alphalens as al

stock_data=pd.read_pickle('D:/stock_data.pkl')
data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','CLOSE_PRICE','VWAP','TURNOVER_VOL']]
data_use=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index()
data_use['volume']=data_use['TURNOVER_VOL']/data_use['VWAP']
sum_close_8_1=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
sum_close_8_2=sum_close_8_1['CLOSE_PRICE'].unstack()
sum_close_8_3=sum_close_8_2.shift().rolling(8).mean()
sum_close_2=sum_close_8_2.shift().rolling(2).mean()

sum_close_table=pd.merge(sum_close_8_3.stack().reset_index(),data_use,on=['TRADE_DATE','TICKER_SYMBOL'])
sum_close_table=sum_close_table.rename(columns={0:'close_8_mean'})
sum_close_table_2=pd.merge(sum_close_2.stack().reset_index(),sum_close_table,on=['TRADE_DATE','TICKER_SYMBOL']).sort_values(by=['TICKER_SYMBOL','TRADE_DATE']).rename(columns={0:'close_2_mean'})
stddev=sum_close_8_2.shift().rolling(8).std()
stddev_table=sum_close_table_2=pd.merge(stddev.stack().reset_index(),sum_close_table_2,on=['TRADE_DATE','TICKER_SYMBOL']).sort_values(by=['TICKER_SYMBOL','TRADE_DATE']).rename(columns={0:'stddev'})

adv_1=sum_close_8_1['volume'].unstack()
adv_2=adv_1.shift().rolling(20).mean()
adv_table=pd.merge(adv_2.stack().reset_index(),stddev_table,on=['TRADE_DATE','TICKER_SYMBOL']).sort_values(by=['TICKER_SYMBOL','TRADE_DATE']).rename(columns={0:'adv20'})
adv_table['volume2adv20']=adv_table['volume']/adv_table['adv20']
adv_table['x3']=1
condition1=((adv_table.volume2adv20>1)|(adv_table.volume2adv20==1))
adv_table['x3']=adv_table['x3'].where(condition1,-1)
adv_table['x2']=1

condition2=(adv_table.close_2_mean<(adv_table.close_8_mean-adv_table.stddev))
adv_table['x2']=adv_table['x2'].where(condition2,adv_table['x3'])
adv_table['alpha21']=adv_table['volume']
condition3=(adv_table.close_2_mean>(adv_table.close_8_mean+adv_table.stddev))
adv_table['alpha21']=adv_table['alpha21'].where(condition3,adv_table['x2'])
adv_table['alpha21']=adv_table['alpha21']*(-1)


final=adv_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])
factor_init = final["alpha21"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=1)

al.tears.create_returns_tear_sheet(factor)
