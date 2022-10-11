import pandas as pd
import numpy as np
import cx_Oracle
import alphalens as al

stock_data= pd.read_pickle('D:/stock_data.pkl')



data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','CLOSE_PRICE','VWAP','TURNOVER_VOL']]

data_use=data_use.set_index(["TICKER_SYMBOL","TRADE_DATE"]).reset_index()
data_use['volume']=data_use['TURNOVER_VOL']/data_use['VWAP']

data_use=data_use.sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
delta_volume_1=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
delta_volume_2=delta_volume_1['volume'].unstack()
delta_volume_3=delta_volume_2-delta_volume_2.shift()

delta_volume_table=pd.merge(delta_volume_3.stack().reset_index(),data_use,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer')


delta_volume_table=delta_volume_table.sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

delta_close_1=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
delta_close_2=delta_close_1['CLOSE_PRICE'].unstack()
delta_close_3=delta_close_2-delta_close_2.shift()

delta_volume_table=delta_volume_table.rename(columns={0:'delta_volumn'})

delta_close_table=pd.merge(delta_close_3.stack().reset_index(),delta_volume_table,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer').sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])


delta_close_table=delta_close_table.rename(columns={0:'delta_close'})

delta_close_table['x1']=np.sign(delta_close_table['delta_volumn'])
delta_close_table['x2']=-1*delta_close_table['delta_close']

delta_close_table['alpha#12']=delta_close_table['x1']*delta_close_table['x2']

final=delta_close_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])
factor_init = final["alpha#12"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)

al.tears.create_returns_tear_sheet(factor)
