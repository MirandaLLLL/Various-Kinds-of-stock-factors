import numpy as np
import pandas as pd
import alphalens as al

stock_data=pd.read_pickle('D:/stock_data.pkl')
data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','CLOSE_PRICE','OPEN_PRICE']]
data_use=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index()

data_use=data_use.sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
data_use['close-open']=data_use['CLOSE_PRICE']-data_use['OPEN_PRICE']
std_1=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
std_2=std_1['close-open'].unstack()
std_3=std_2.abs()
std_4=std_3.shift().rolling(5).std()
std_table=pd.merge(std_4.stack().reset_index(),data_use,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'std'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

corr_1=std_table[['CLOSE_PRICE','OPEN_PRICE']].rolling(10).corr()
corr_2=corr_1.reset_index()
corr_3=corr_2.drop_duplicates(subset={'level_0'},keep='last')
corr_4=corr_3.set_index(["level_0","level_1"]).reset_index()
corr_4=corr_4.drop(columns={'level_1','OPEN_PRICE'}).rename(columns={'CLOSE_PRICE':'corr'})
std_table=std_table.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index()
std_table['level_0']=corr_4['level_0']
corr_table=pd.merge(std_table,corr_4,on=['level_0']).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

condition=corr_table.TICKER_SYMBOL==corr_table.shift(10).TICKER_SYMBOL
corr_table['corr']=corr_table['corr'].where(condition,np.nan)


corr_table['x1']=corr_table['std']+corr_table['close-open']
rank_1=corr_table.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_2=rank_1['x1'].unstack()
rank_3=(rank_2.rank()-1)/(len(rank_2)-1)
rank_table=pd.merge(corr_table,rank_3.stack().reset_index(),on=['TRADE_DATE','TICKER_SYMBOL']).sort_values(by=['TICKER_SYMBOL','TRADE_DATE']).rename(columns={0:'alpha#18'})
rank_table['alpha#18']=rank_table['alpha#18']*(-1)

final=rank_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])
factor_init = final["alpha#18"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=1)

al.tears.create_returns_tear_sheet(factor)

ic=al.performance.factor_information_coefficient(factor)
