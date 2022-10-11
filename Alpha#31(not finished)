import numpy as np
import pandas as pd
import alphalens as al

stock_data=pd.read_pickle('D:/stock_data.pkl')

data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','CLOSE_PRICE','VWAP','TURNOVER_VOL','LOWEST_PRICE']]
data_use=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index().sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
data_use['volume']=data_use['TURNOVER_VOL']/data_use['VWAP']
#+ sign(correlation(adv20, low, 12)))
delta_1=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
delta_2=delta_1['CLOSE_PRICE'].unstack()
delta_3=delta_2-delta_2.shift(10)
delta_table=pd.merge(delta_3.stack().reset_index(),data_use,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'delta_close'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
rank_1=delta_table.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_2=rank_1['delta_close'].unstack()
rank_3=(rank_2.rank()-1)/(len(rank_2)-1)
rank_4=(rank_3.rank()-1)/(len(rank_3)-1)*(-1)
rank_table=pd.merge(rank_4.stack().reset_index(),delta_table,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'rank'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

def rank(array):
    s = pd.Series(array)
    return s.rank()[len(s)-1]

delay_linear_1=rank_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])
delay_linear_2=delay_linear_1['rank'].unstack()
delay_linear_3=(10-delay_linear_2.rolling(10).apply(rank))/9
delay_linear_table=pd.merge(delay_linear_3.stack().reset_index(),rank_table,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'delay_linear'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

rank_delay_linear_1=delay_linear_table.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_delay_linear_2=rank_delay_linear_1['delay_linear'].unstack()
rank_delay_linear_3=(rank_delay_linear_2.rank()-1)/(len(rank_delay_linear_2)-1)
rank_delay_linear_4=(rank_delay_linear_3.rank()-1)/(len(rank_delay_linear_3)-1)
rank_delay_table=pd.merge(rank_delay_linear_4.stack().reset_index(),delay_linear_table,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'rank_delay_linear'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

del_1=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
del_2=del_1['CLOSE_PRICE'].unstack()
del_3=(del_2-del_2.shift(3))*(-1)
del_table=pd.merge(del_3.stack().reset_index(),rank_delay_table,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'del'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

rank_y_1=del_table.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_y_2=rank_y_1['del'].unstack()
rank_y_3=(rank_y_2.rank()-1)/(len(rank_y_2)-1)
rank_bond_1=rank_y_3+rank_delay_linear_4
rank_bond_2=(rank_bond_1.rank()-1)/(len(rank_bond_1)-1)
rank_y_table=pd.merge(rank_y_3.stack().reset_index(),del_table,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'rank_y'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

rank_bond_table=pd.merge(rank_bond_2.stack().reset_index(),rank_y_table,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'rank_bond'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
adv_1=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
adv_2=adv_1['volume'].unstack()
adv_3=adv_2.shift().rolling(20).mean()
adv_table=pd.merge(adv_3.stack().reset_index(),rank_bond_table,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'adv20'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])



corr_1=adv_table[['adv20','LOWEST_PRICE']]
corr_2=corr_1.shift().rolling(12).corr().reset_index()
corr_3=corr_2.drop_duplicates(subset={'level_0'},keep='last').set_index(["level_0","level_1"]).reset_index().rename(columns={'adv20':'corr_adv'}).drop(columns={'level_1','LOWEST_PRICE'})


adv_table=adv_table.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index()
adv_table['level_0']=corr_3['level_0']
corr_adv_table=pd.merge(adv_table,corr_3,on=['level_0']).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

condition=corr_adv_table.TICKER_SYMBOL==corr_adv_table.shift(13).TICKER_SYMBOL
corr_adv_table['corr_adv']=corr_adv_table['corr_adv'].where(condition,np.nan)
corr_adv_table['z3']=

final=ts_max_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])
factor_init = final["alpha#31"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=1)

al.tears.create_returns_tear_sheet(factor)

ic=al.performance.factor_information_coefficient(factor)
