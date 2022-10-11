import numpy as np
import pandas as pd
import alphalens as al

stock_data=pd.read_pickle('D:/stock_data.pkl')

data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','CLOSE_PRICE']]
data_use=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index().sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])



rank_set=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
rank_un=rank_set['CLOSE_PRICE'].unstack()
delta_5=rank_un-rank_un.shift(5)
table_1=pd.merge(delta_5.stack().reset_index(),data_use,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'delta_5'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
table_set=table_1.set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_1=table_set['delta_5'].unstack()
rank_2=((rank_1.rank()-1)/(len(rank_1)-1))*(-1)
rank_3=((rank_2).rank()-1)/(len(rank_2)-1)
rank_4=((rank_3).rank()-1)/(len(rank_3)-1)
rank_table=pd.merge(rank_4.stack().reset_index(),table_1,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'rank_x1'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
ts_set=rank_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])
ts_1=ts_set['rank_x1'].unstack()
ts_2=ts_1.shift().rolling(2).min()
ts_3=ts_2.rolling(2).sum()
ts_table=pd.merge(ts_3.stack().reset_index(),rank_table,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'ts_3'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
r_1=ts_table.set_index(["TICKER_SYMBOL","TRADE_DATE"])
r_2=r_1['ts_3'].unstack()
r_3=(r_2.rank()-1)/(len(r_2)-1)
r_4=(r_3.rank()-1)/(len(r_3)-1)
r_5=r_4*r_4.shift()
r_table=pd.merge(r_5.stack().reset_index(),ts_table,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'r_5'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
min_1=r_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])
min_2=min_1['r_5'].unstack()
min_3=min_2.shift().rolling(5).min()
min_table=pd.merge(min_3.stack().reset_index(),r_table,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'min'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

returns_1=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
returns_2=returns_1['CLOSE_PRICE'].unstack()
returns_3=(returns_2-returns_2.shift())*(-1)
return_rate=(-1)*(returns_2-returns_2.shift())/returns_2.shift()
delay_re=returns_3.shift(6)
delay_rate=return_rate.shift(6)

def rank(array):
    s = pd.Series(array)
    return s.rank()[len(s)-1]


ts_re=(delay_re.rolling(5).apply(rank)-1)/4
ts_rate=(delay_rate.rolling(5).apply(rank)-1)/4

ts_table_1=pd.merge(ts_re.stack().reset_index(),min_table,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'ts_re'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
ts_table_2=pd.merge(ts_rate.stack().reset_index(),ts_table_1,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'ts_rate'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

ts_table_2['alpha#29']=ts_table_2['min']+ts_table_2['ts_re']
final=ts_table_2.set_index(["TRADE_DATE","TICKER_SYMBOL"])




factor_init = final["alpha#29"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=1)

al.tears.create_returns_tear_sheet(factor)

ic=al.performance.factor_information_coefficient(factor)
