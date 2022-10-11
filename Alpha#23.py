import numpy as np
import pandas as pd
import alphalens as al

stock_data=pd.read_pickle('D:/stock_data.pkl')
data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','CLOSE_PRICE','HIGHEST_PRICE']]
data_use=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index()
sum_high_1=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])

sum_high_2=sum_high_1['HIGHEST_PRICE'].unstack()
sum_high_3=sum_high_2.shift().rolling(20).mean()
delta_high=sum_high_2-sum_high_2.shift(2)
table_1=pd.merge(sum_high_3.stack().reset_index(),data_use,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'sum_high'})
table_2=pd.merge(delta_high.stack().reset_index(),table_1,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'delta_high'})

table_2['alpha#23']=table_2['delta_high']*(-1)
condition=table_2.sum_high>table_2.HIGHEST_PRICE
table_2['alpha#23']=table_2['alpha#23'].where(condition,0)

final=table_2.set_index(["TRADE_DATE","TICKER_SYMBOL"])
factor_init = final["alpha#23"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=1)

al.tears.create_returns_tear_sheet(factor)

ic=al.performance.factor_information_coefficient(factor)
