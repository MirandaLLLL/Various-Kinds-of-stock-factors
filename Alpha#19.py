import numpy as np
import pandas as pd
import alphalens as al

stock_data=pd.read_pickle('D:/stock_data.pkl')
data_use=stock_data[['TRADE_DATE','TICKER_SYMBOL','CLOSE_PRICE']]
data_use=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"]).reset_index()
delta_7_1=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
delta_7_2=delta_7_1['CLOSE_PRICE'].unstack()
delta_7_3=delta_7_2-delta_7_2.shift(7)
delta_table=pd.merge(delta_7_3.stack().reset_index(),data_use,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'delta_7'})
delay_7=delta_7_2.shift(7)

sign_1=np.sign(delta_7_2-delay_7)
sign_table=pd.merge(sign_1.stack().reset_index(),delta_table,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'sign'})
sign_table['x1']=sign_table['sign']*(-1)+sign_table['delta_7']
returns=delta_7_2-delta_7_2.shift()
return_rate=(delta_7_2-delta_7_2.shift())/delta_7_2.shift()
sum_returns=returns.rolling(250).sum()
sum_return_rate=return_rate.rolling(250).sum()+1
rank_returns_1=sum_returns.stack().reset_index().set_index(["TICKER_SYMBOL","TRADE_DATE"]).unstack().rank()-1
rank_returns_2=rank_returns_1/(len(rank_returns_1)-1)+1
rank_return_rate_1=sum_return_rate.stack().reset_index().set_index(["TICKER_SYMBOL","TRADE_DATE"]).unstack().rank()-1
rank_return_rate_2=rank_return_rate_1/(len(rank_return_rate_1)-1)+1
rank_table=pd.merge(rank_return_rate_2.stack().reset_index(),sign_table,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'x2_rate'})
rank_table_2=pd.merge(rank_returns_2.stack().reset_index(),rank_table,on=['TRADE_DATE','TICKER_SYMBOL']).rename(columns={0:'x2_returns'})

rank_table_2['alpha#19_returns']=rank_table_2['x1']*rank_table_2['x2_returns']*(-1)
rank_table_2['alpha#19_rate']=rank_table_2['x1']*rank_table_2['x2_rate']*(-1)
final=rank_table_2.set_index(["TRADE_DATE","TICKER_SYMBOL"])
factor_init = final["alpha#19_returns"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)

al.tears.create_returns_tear_sheet(factor)
