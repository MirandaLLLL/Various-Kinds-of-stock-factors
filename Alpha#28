import numpy as np
import pandas as pd
import alphalens as al

stock_data=pd.read_pickle('D:/stock_data.pkl')

data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','CLOSE_PRICE','HIGHEST_PRICE','LOWEST_PRICE','VWAP','TURNOVER_VOL']]
data_use=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index().sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

data_use['volume']=data_use['TURNOVER_VOL']/data_use['VWAP']
adv_1=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
adv_2=adv_1['volume'].unstack()
adv_3=adv_2.shift().rolling(20).mean()
adv_table=pd.merge(adv_3.stack().reset_index(),data_use,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'adv'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

corr_1=adv_table[['adv','LOWEST_PRICE']]
corr_2=corr_1.shift().rolling(5).corr()

corr_3=corr_2.reset_index().drop_duplicates(subset={'level_0'},keep='last').set_index(["level_0","level_1"]).reset_index().rename(columns={'adv':'corr'})
corr_4=corr_3.drop(columns={'level_1','LOWEST_PRICE'})
adv_table=adv_table.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index().sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

adv_table['level_0']=corr_4['level_0']

corr_table=pd.merge(corr_4,adv_table,on=['level_0']).rename(columns={0:'corr'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
condition=corr_table.TICKER_SYMBOL==corr_table.shift(6).TICKER_SYMBOL
corr_table['corr']=corr_table['corr'].where(condition,np.nan)
corr_table['alpha#28']=corr_table['corr']+(corr_table['HIGHEST_PRICE']+corr_table['LOWEST_PRICE'])/2#-corr_table['CLOSE_PRICE']
final=corr_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])

factor_init = final["alpha#28"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=1)

al.tears.create_returns_tear_sheet(factor)

ic=al.performance.factor_information_coefficient(factor)
