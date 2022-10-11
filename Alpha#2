import pandas as pd
import numpy as np
import cx_Oracle
import alphalens as al


wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_VAL_HIGH,S_DQ_LOW,S_DQ_OPEN,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20100930'
 '''
stock_data= pd.read_sql(sqlcmd, con=wd)

data_use=stock_data[['TICKER_SYMBOL','TRADE_DATE','OPEN_PRICE','CLOSE_PRICE','VWAP']]

data_use['x1']=data_use['VWAP'].apply(np.log2)
data_use_copy=data_use.copy()
data_use=data_use.set_index(["TRADE_DATE","TICKER_SYMBOL"])
data_un=data_use['x1'].unstack()
x2_un=data_un-data_un.shift(2)

x2=x2_un.stack().reset_index()
x2_table=pd.merge(x2,data_use_copy,on=['TICKER_SYMBOL','TRADE_DATE'])
x2_table=x2_table.rename(columns={0:'x2'})

x2_table['y1']=(x2_table['CLOSE_PRICE']-x2_table['OPEN_PRICE'])/x2_table['OPEN_PRICE']

x2_table=x2_table.sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
y1_table_copy=x2_table.copy()
x2_table=x2_table.set_index(["TICKER_SYMBOL",'TRADE_DATE'])
x3_un=x2_table['x2'].unstack()

rank_un=x3_un.rank()

rank=rank_un.stack().reset_index()
rank_table=pd.merge(rank,y1_table_copy)

rank_table=rank_table.rename(columns={0:'x3'})

rank_table_copy=rank_table.copy()
rank_table=rank_table.set_index(["TICKER_SYMBOL","TRADE_DATE"])
y2_un=rank_table['y1'].unstack()

y2_c=(y2_un.rank()-1)/len(y2_un)

y2_re=y2_c.stack().reset_index()
y2_table=pd.merge(y2_re,rank_table_copy)

y2_table=y2_table.rename(columns={0:'y2'})

y2_table_copy=y2_table.copy()
y2_table=y2_table.sort_values(by=['TICKER_SYMBOL','TRADE_DATE']).set_index(["TRADE_DATE","TICKER_SYMBOL"])

corr_table=y2_table_copy[['TICKER_SYMBOL','TRADE_DATE','y2','x3']]
corr_table=corr_table.sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
corr_xy=corr_table[['y2','x3']]

tese=corr_xy.shift().rolling(6).corr()
test=tese.reset_index().drop_duplicates(subset={'level_0'},keep='last')
test=test.drop(columns={'x3','level_1'})

test=test.rename(columns={'y2':'corr'})
test['index']=test['level_0'].rank()
corr_table['index']=test['level_0'].rank()
test_merge=pd.merge(test,corr_table)

test_merge=test_merge.drop(columns={'level_0','index'})

test_merge['alpha#2']=test_merge['corr']*(-1)

final=pd.merge(test_merge,data_use_copy,on=['TICKER_SYMBOL','TRADE_DATE']).set_index(["TRADE_DATE","TICKER_SYMBOL"])

factor_init = final["alpha#2"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)

al.tears.create_returns_tear_sheet(factor)
