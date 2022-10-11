import pandas as pd
import numpy as np
import cx_Oracle
import alphalens as al

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_HIGH,S_DQ_LOW,S_DQ_OPEN,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20181231'
 '''
stock_data = pd.read_sql(sqlcmd, con=wd)

stock_data=stock_data.rename(columns={'S_INFO_WINDCODE':'TICKER_SYMBOL','TRADE_DT':'TRADE_DATE','S_DQ_CLOSE':'CLOSE_PRICE'})

stock_data=stock_data.sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])
stock_data_copy=stock_data.copy()
stock_data=stock_data.set_index(["TRADE_DATE","TICKER_SYMBOL"])
Return=stock_data['CLOSE_PRICE'].unstack()

Return_c=Return.diff(1)/Return.shift(1)
Return_re=Return_c.stack().reset_index()
Select_Table=stock_data_copy[['TICKER_SYMBOL','TRADE_DATE','CLOSE_PRICE']]
Return_table=pd.merge(Return_re,Select_Table,on=['TICKER_SYMBOL','TRADE_DATE'],how='outer')

Return_table=Return_table.rename(columns={0:'returns'}).sort_values(by=['TICKER_SYMBOL','TRADE_DATE'])

test=Return_table.set_index(["TRADE_DATE","TICKER_SYMBOL"])
test=test['CLOSE_PRICE'].unstack()

stock_std=test.shift().rolling(20).std()

std_data=stock_std.stack().reset_index()
std_table=pd.merge(std_data,Return_table,on=['TICKER_SYMBOL','TRADE_DATE'])

std_table=std_table.rename(columns={0:'std'})

condition=std_table.returns<0
std_table_copy=std_table.copy()
std_table['std']=std_table['std'].where(condition,std_table['CLOSE_PRICE'])
std_table=std_table.rename(columns={'std':'x1'})

std_table['x2']=std_table['x1']*std_table['x1']

std_table_update_copy=std_table.copy()

std_table["TRADE_DATE"]=pd.to_datetime(std_table["TRADE_DATE"])

std_table=std_table.set_index(['TRADE_DATE','TICKER_SYMBOL'])
x2_un=std_table['x2'].unstack()

x3_un=5-(x2_un.shift().rolling(5,closed='left').apply(np.argmax))

x3_data=x3_un.stack().reset_index()
std_table_update_copy["TRADE_DATE"]=pd.to_datetime(std_table_update_copy["TRADE_DATE"])
index_table=pd.merge(x3_data,std_table_update_copy,on=['TICKER_SYMBOL','TRADE_DATE'])

index_table=index_table.rename(columns={0:'index'})

index_table_copy=index_table.copy()
rank_table=index_table.set_index(["TICKER_SYMBOL","TRADE_DATE"])

rank_un=rank_table['index'].unstack()


rank_after=rank_un.rank()
rank_2=(rank_after-1)/(len(rank_after)-1)-0.5

rank_re=rank_2.stack().reset_index()
final=pd.merge(rank_re,index_table_copy,on=['TICKER_SYMBOL','TRADE_DATE'])
final=final.rename(columns={0:'x4'})
final=final.set_index(['TRADE_DATE','TICKER_SYMBOL'])
final=final.reset_index().rename(columns={'TRADE_DATE':'TRADE_DT','TICKER_SYMBOL':'S_INFO_WINDCODE'}).set_index(['TRADE_DT','S_INFO_WINDCODE'])

factor_init = final["x4"].copy()
price_df = final["CLOSE_PRICE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)

al.tears.create_returns_tear_sheet(factor)
