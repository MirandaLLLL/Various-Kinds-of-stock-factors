import pandas as pd
import numpy as np
import cx_Oracle
import alphalens as al

stock_data= pd.read_pickle('D:/stock_data.pkl')



data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','CLOSE_PRICE','LOWEST_PRICE']]

data_use=data_use.set_index(["TICKER_SYMBOL","TRADE_DATE"]).reset_index()
data_use=data_use.sort_values(by=["TICKER_SYMBOL","TRADE_DATE"]).set_index(["TICKER_SYMBOL","TRADE_DATE"])
rank_low_1=data_use['LOWEST_PRICE'].unstack()
rank_low_2=(rank_low_1.rank()-1)/len(rank_low_1)

rank_low_table=pd.merge(data_use,rank_low_2.stack().reset_index(),on=['TICKER_SYMBOL','TRADE_DATE'],how='outer')

Ts_rank=rank_low_table.sort_values(by=['TICKER_SYMBOL','TRADE_DATE']).set_index(["TRADE_DATE","TICKER_SYMBOL"]).rename(columns={0:'rank_low'})


Ts_rank_1=Ts_rank['rank_low'].unstack()


def rank(array):
    s = pd.Series(array)
    return s.rank()[len(s)-1]

t=(Ts_rank_1.rolling(9).apply(rank)-1)/8

final=pd.merge(t.stack().reset_index(),Ts_rank.reset_index(),on=['TICKER_SYMBOL','TRADE_DATE'],how='outer')
final=final.rename(columns={0:'alpha#4'})
final['alpha#4']=final['alpha#4']*(-1)
final=final.sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

final=final.set_index(["TRADE_DATE","TICKER_SYMBOL"])


factor_init = final["alpha#4"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=1)

al.tears.create_returns_tear_sheet(factor)
