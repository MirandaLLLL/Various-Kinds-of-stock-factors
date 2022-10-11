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
sqlcmd = '''select S_INFO_WINDCODE,REPORT_PERIOD,S_FA_OP_TTM,ANN_DT from AShareTTMHis  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and REPORT_PERIOD>'20100929'
 '''
Open_Profit_TTM = pd.read_sql(sqlcmd, con=wd)


Open_Profit_TTM["REPORT_PERIOD"] = pd.to_datetime(Open_Profit_TTM["REPORT_PERIOD"])
Open_Profit_TTM["ANN_DT"] = pd.to_datetime(Open_Profit_TTM["ANN_DT"])
Open_Profit_TTM["REPORT_PERIOD"] = pd.to_datetime(Open_Profit_TTM["REPORT_PERIOD"])
TradeDate["TRADE_DT"] = pd.to_datetime(TradeDate["TRADE_DT"])

Open_Profit_TTM=Open_Profit_TTM.sort_values(by=['S_INFO_WINDCODE','REPORT_PERIOD'])

Open_Profit_TTM_copy=Open_Profit_TTM.copy()
Open_Profit_TTM=Open_Profit_TTM_copy.set_index(["REPORT_PERIOD","S_INFO_WINDCODE"])
Open_Profit_TTM=Open_Profit_TTM['S_FA_OP_TTM'].unstack()

m = Open_Profit_TTM.index.get_level_values(level='REPORT_PERIOD').quarter == 1
mm = np.repeat(m, Open_Profit_TTM.shape[1], axis=None).reshape(Open_Profit_TTM.shape[0], Open_Profit_TTM.shape[1])
Open_Profit_Per_TTM = Open_Profit_TTM.where(mm, Open_Profit_TTM.diff())
Open_Profit_Per_TTM_RE=Open_Profit_Per_TTM.stack().reset_index()
Open_Profit_Per_TTM_merge=pd.merge(Open_Profit_Per_TTM_RE,Open_Profit_TTM_copy,on=['S_INFO_WINDCODE','REPORT_PERIOD'])

Open_Profit_Per_TTM_merge=Open_Profit_Per_TTM_merge.rename(columns={0:'Per_OP_TTM'})
Open_Profit_Per_TTM_merge=Open_Profit_Per_TTM_merge.sort_values(by=["S_INFO_WINDCODE","REPORT_PERIOD"])

Open_Profit_Per_TTM_merge_copy=Open_Profit_Per_TTM_merge.copy()
Open_Profit_Per_TTM_merge=Open_Profit_Per_TTM_merge.set_index(['REPORT_PERIOD','S_INFO_WINDCODE'])
Open_Profit_Per_TTM_merge=Open_Profit_Per_TTM_merge['Per_OP_TTM'].unstack()
Open_Profit_Rate=Open_Profit_Per_TTM_merge/abs(Open_Profit_Per_TTM_merge.shift(4))

Open_Profit_Rate_re=Open_Profit_Rate.stack().reset_index()
Open_rate_merge=pd.merge(Open_Profit_Rate_re,Open_Profit_Per_TTM_merge_copy,on=['S_INFO_WINDCODE','REPORT_PERIOD'])

Open_rate_merge=Open_rate_merge.rename(columns={0:'Rate'})

s=(Open_rate_merge.S_INFO_WINDCODE==Open_rate_merge.shift(-1).S_INFO_WINDCODE)&(Open_rate_merge.ANN_DT>Open_rate_merge.shift(-1).ANN_DT)
Open_rate_merge_copy=Open_rate_merge.copy().sort_values(by=['S_INFO_WINDCODE','REPORT_PERIOD'])
Open_rate_merge['Rate']=Open_rate_merge['Rate'].where(~s,Open_rate_merge['Rate'].shift(-1))
Open_rate_merge=Open_rate_merge.rename(columns={'ANN_DT':'TRADE_DT'})

merge_T=pd.merge(Open_rate_merge,TradeDate,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')

merge_T_copy=merge_T.copy()
merge_T=merge_T.drop_duplicates(subset=['TRADE_DT','S_INFO_WINDCODE'],keep='last')
merge_T=merge_T.set_index(['TRADE_DT','S_INFO_WINDCODE'])

merge_T_FILL=merge_T['Rate'].unstack()
merge_T_FILL=merge_T_FILL.fillna(method='ffill')

merge_T_RE=merge_T_FILL.stack().reset_index()
Final=pd.merge(merge_T_RE,TradeDate,on=['TRADE_DT','S_INFO_WINDCODE'],how='right')

Final=Final.rename(columns={0:'Rate'})

Final=Final.set_index(["TRADE_DT","S_INFO_WINDCODE"])


factor_init = Final["Rate"].copy()
price_df = Final["S_DQ_CLOSE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)

al.tears.create_returns_tear_sheet(factor)



