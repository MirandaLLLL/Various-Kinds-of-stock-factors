import pandas as pd
import cx_Oracle
import calendar
import alphalens as al
import numpy as np
import datetime

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_VAL_PE_TTM,S_DQ_CLOSE_TODAY from AShareEODDerivativeIndicator  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20100101'
 '''
PE_table = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,ANN_DT,REPORT_PERIOD,STATEMENT_TYPE,NET_PROFIT_INCL_MIN_INT_INC,NET_PROFIT_EXCL_MIN_INT_INC from AShareIncome  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and ANN_DT>'20090101'and STATEMENT_TYPE='408002000' 
 '''
Net_Profit = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20100101'
 '''
TradeDate = pd.read_sql(sqlcmd, con=wd)

Net_Profit["REPORT_PERIOD"] = pd.to_datetime(Net_Profit["REPORT_PERIOD"])
PE_table["TRADE_DT"]=pd.to_datetime(PE_table["TRADE_DT"])
Net_Profit["ANN_DT"] = pd.to_datetime(Net_Profit["ANN_DT"])
TradeDate["TRADE_DT"]=pd.to_datetime(TradeDate["TRADE_DT"])

Net_Profit=Net_Profit.drop(columns='STATEMENT_TYPE').sort_values(by=["S_INFO_WINDCODE","REPORT_PERIOD"])

Net_Profit_copy=Net_Profit.copy()
T1=Net_Profit.set_index(["REPORT_PERIOD","S_INFO_WINDCODE"])
T2=T1['NET_PROFIT_INCL_MIN_INT_INC'].unstack()
T3=T2.copy()
T3=(T3-T3.shift(4))/abs(T3.shift(4))
T4=T3.stack().reset_index()
T5=pd.merge(T4,Net_Profit_copy)
T6=T5.drop(columns={"NET_PROFIT_INCL_MIN_INT_INC","NET_PROFIT_EXCL_MIN_INT_INC"}).rename(columns={0:'IN_growth'})

T6=T6.sort_values(by=["S_INFO_WINDCODE","REPORT_PERIOD"])
M=T6.copy()
MM=M.ANN_DT<=M.shift(-1).ANN_DT
MM_re=M.ANN_DT>M.shift(-1).ANN_DT
MM_plus=(M.S_INFO_WINDCODE!=M.shift(-1).S_INFO_WINDCODE)
CONDI=MM|(MM_re&MM_plus)
T6["IN_growth"]=T6["IN_growth"].where(CONDI,T6["IN_growth"].shift(-1))

T7=T6.rename(columns={'ANN_DT':'TRADE_DT'})
T8=pd.merge(T7,PE_table,how='outer')
T8=T8.drop_duplicates(subset=['TRADE_DT', 'S_INFO_WINDCODE'], keep='last')
T8=T8.set_index(["TRADE_DT","S_INFO_WINDCODE"])
T9=T8["IN_growth"].unstack().fillna(method='ffill')

T10=T9.stack().reset_index()

T11=pd.merge(PE_table,T10,how='left')
T12=pd.merge(TradeDate,T11,how='left')
T12['PEG_in']=T12['S_VAL_PE_TTM']/T12[0]
T12=T12.set_index(["TRADE_DT","S_INFO_WINDCODE"])

factor_init = T12["PEG_in"].copy()
price_df = T12["S_DQ_CLOSE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)

al.tears.create_returns_tear_sheet(factor)
