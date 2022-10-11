import pandas as pd
import cx_Oracle
import calendar
import alphalens as al
import numpy as np

stock_data=pd.read_pickle('D:/stock_data.pkl')

stock_data['volume']=stock_data['TURNOVER_VOL']/stock_data['VWAP']
data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','CLOSE_PRICE','volume']]

data_use=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index()
delta_close_7=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
delta_un=delta_close_7['CLOSE_PRICE'].unstack()
delta_un=delta_un-delta_un.shift(7)
delta_table=pd.merge(delta_un.stack().reset_index(),data_use,on=['TICKER_SYMBOL','TRADE_DATE'])
delta_table=delta_table.rename(columns={0:'delta_close_7'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
delta_table['sign_delta']=np.sign(delta_table['delta_close_7'])
delta_table['abs_delta']=abs(delta_table['delta_close_7'])
adv_1=delta_close_7['volume'].unstack()

adv_2=adv_1.shift().rolling(20).mean()
adv_table=pd.merge(adv_2.stack().reset_index(),delta_table,on=['TICKER_SYMBOL','TRADE_DATE'])
adv_table=adv_table.rename(columns={0:'adv20'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

def rank(array):
    s = pd.Series(array)
    return s.rank()[len(s)-1]

ts_rank_1=adv_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])

ts_rank_2=ts_rank_1['abs_delta'].unstack()
ts_rank_3=(ts_rank_2.rolling(60).apply(rank)-1)/(-60)
ts_rank_4=ts_rank_3.stack().reset_index()
ts_rank_table=pd.merge(ts_rank_4,adv_table,on=['TICKER_SYMBOL','TRADE_DATE'])
ts_rank_table=ts_rank_table.rename(columns={0:'ts_rank'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
ts_rank_table['alpha_x']=ts_rank_table['ts_rank']*ts_rank_table['sign_delta']
ts_rank_table['alpha#7']=ts_rank_table['alpha_x']
condition=(ts_rank_table['adv20']<ts_rank_table['volume'])
ts_rank_table['alpha#7']=ts_rank_table['alpha#7'].where(condition,-1)

factor_init = final["alpha#7"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=1)

al.tears.create_returns_tear_sheet(factor)

ic=al.performance.factor_information_coefficient(factor)

final=ts_rank_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])
