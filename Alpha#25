import numpy as np
import pandas as pd
import alphalens as al

stock_data=pd.read_pickle('D:/stock_data.pkl')

data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','CLOSE_PRICE','VWAP','TURNOVER_VOL','HIGHEST_PRICE']]
data_use=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index()

data_use['volume']=data_use['TURNOVER_VOL']/data_use['VWAP']
data_use['high-close']=data_use['HIGHEST_PRICE']-data_use['CLOSE_PRICE']

return_rate_1=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
return_rate_2=return_rate_1['CLOSE_PRICE'].unstack()
return_rate_3=(return_rate_2-return_rate_2.shift())/return_rate_2.shift()
returns_3=(return_rate_2-return_rate_2.shift())
return_table_1=pd.merge(return_rate_3.stack().reset_index(),data_use,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'return_rate'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
return_table_2=pd.merge(returns_3.stack().reset_index(),return_table_1,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'returns'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])


adv_1=return_table_2.set_index(["TRADE_DATE","TICKER_SYMBOL"])
adv_2=adv_1['volume'].unstack()
adv_3=adv_2.shift().rolling(20).mean()
adv_table=pd.merge(adv_3.stack().reset_index(),return_table_2,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'adv'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])


adv_table['x1']=adv_table['adv']*adv_table['VWAP']*adv_table['returns']*adv_table['high-close']/adv_table['CLOSE_PRICE']


rank_1=adv_table.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_2=rank_1['x1'].unstack()
rank_3=(rank_2.rank()-1)/(len(rank_2)-1)
final=pd.merge(rank_3.stack().reset_index(),adv_table,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'alpha#25'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
final['alpha#25']=final['alpha#25']*(-1)
final=final.set_index(["TRADE_DATE","TICKER_SYMBOL"])


factor_init = final["alpha#25"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=1)

al.tears.create_returns_tear_sheet(factor)

ic=al.performance.factor_information_coefficient(factor)
