import pandas as pd
import cx_Oracle
import calendar
import alphalens as al
import numpy as np

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20100930'
 '''
TradeDate = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,REPORT_PERIOD,ANN_DT，S_FA_TOTALEQUITY_MRQ,NET_PROFIT_TTM from AShareTTMHis  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and REPORT_PERIOD>'20100101'
 '''
Factor_Base = pd.read_sql(sqlcmd, con=wd)

Factor_Base.sort_values(by=['S_INFO_WINDCODE','REPORT_PERIOD'])

TOTALEQUITY=Factor_Base[['S_INFO_WINDCODE','REPORT_PERIOD','S_FA_TOTALEQUITY_MRQ']]
TOTALEQUITY_un=TOTALEQUITY.set_index(["REPORT_PERIOD","S_INFO_WINDCODE"]).unstack()

TOTALEQUITY_C=TOTALEQUITY_un/TOTALEQUITY_un.shift(4)

T_RE=TOTALEQUITY_C.stack().reset_index()

t_table=pd.merge(T_RE,Factor_Base,on=['S_INFO_WINDCODE','REPORT_PERIOD'],how='outer')

t_table=t_table.rename(columns={'S_FA_TOTALEQUITY_MRQ_x':'After_TE','S_FA_TOTALEQUITY_MRQ_y':'S_FA_TOTALEQUITY_MRQ'})

t_table=t_table.sort_values(by=['S_INFO_WINDCODE','REPORT_PERIOD'])

s=(t_table.S_INFO_WINDCODE==t_table.shift(-1).S_INFO_WINDCODE)&(t_table.ANN_DT>t_table.shift(-1).ANN_DT)
AfterTurn=t_table.copy()
AfterTurn['After_TE']=AfterTurn['After_TE'].where(~s,AfterTurn['After_TE'].shift(-1))
AfterTurn['NET_PROFIT_TTM']=AfterTurn['NET_PROFIT_TTM'].where(~s,AfterTurn['NET_PROFIT_TTM'].shift(-1))

AfterTurn['NetP/TE']=AfterTurn['NET_PROFIT_TTM']/AfterTurn['After_TE']

use_data=AfterTurn[['S_INFO_WINDCODE','ANN_DT','NetP/TE']].rename(columns={'ANN_DT':'TRADE_DT'})

merge_1=pd.merge(use_data,TradeDate,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')

merge_2=merge_1.drop_duplicates(subset=['TRADE_DT','S_INFO_WINDCODE'],keep='last').sort_values(by=['S_INFO_WINDCODE','TRADE_DT']).set_index(["TRADE_DT","S_INFO_WINDCODE"])
merge_un=merge_2['NetP/TE'].unstack()
merge_fill=merge_un.fillna(method='ffill')

merge_3=merge_fill.stack().reset_index()

merge_4=pd.merge(merge_3,TradeDate,on=['S_INFO_WINDCODE','TRADE_DT'],how='right')

final=merge_4.rename(columns={0:'NetP/TE'})
final["TRADE_DT"] = pd.to_datetime(final["TRADE_DT"])
final=final.set_index(["TRADE_DT","S_INFO_WINDCODE"])

factor_init = final["NetP/TE"].copy()
price_df = final["S_DQ_CLOSE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)
al.tears.create_returns_tear_sheet(factor)

