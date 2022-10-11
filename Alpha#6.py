import pandas as pd
import cx_Oracle
import calendar
import alphalens as al
import numpy as np

stock_data=pd.read_pickle('D:/stock_data.pkl')

stock_data['volume']=stock_data['TURNOVER_VOL']/stock_data['VWAP']
data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','OPEN_PRICE','CLOSE_PRICE','volume']]

data_use=data_use.sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

data_use_corr=data_use[['OPEN_PRICE','volume']]

corr_1=data_use_corr.shift().rolling(10).corr()

corr_2=corr_1.reset_index()

corr_3=corr_2.drop_duplicates(subset={'level_0'},keep='last')


corr_4=corr_3.drop(columns={'level_1','volume'})

corr_4['index']=corr_4['level_0'].rank()
data_use['index']=corr_4['level_0'].rank()

corr_table=pd.merge(corr_4,data_use,on=['index'])

corr_table=corr_table.rename(columns={'OPEN_PRICE_x':'corr'}).drop(columns={'index','level_0'})

corr_table['alpha#6']=corr_table['corr']*(-1)

final=corr_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])
factor_init = final["alpha#6"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)

al.tears.create_returns_tear_sheet(factor)
ic=al.performance.factor_information_coefficient(factor)
