import pandas as pd
import cx_Oracle
import calendar
import alphalens as al
import numpy as np

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20100101'
 '''
TradeDate = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,REPORT_PERIOD,ANN_DT，S_QFA_YOYOP，S_QFA_CGROP，S_QFA_YOYPROFIT，S_QFA_CGRPROFIT，S_QFA_YOYSALES，S_QFA_CGRSALES，S_FA_DEDUCTEDPROFIT from AShareFinancialIndicator   where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and REPORT_PERIOD>'20100101'
 '''
Factor_Base = pd.read_sql(sqlcmd, con=wd)

Factor_Base_copy=Factor_Base.copy()
s=(Factor_Base_copy.S_INFO_WINDCODE==Factor_Base_copy.shift(-1).S_INFO_WINDCODE)&(Factor_Base_copy.ANN_DT>Factor_Base_copy.shift(-1).ANN_DT)
AfterTurn=Factor_Base_copy.copy()
AfterTurn['S_QFA_YOYOP']=AfterTurn['S_QFA_YOYOP'].where(~s,AfterTurn['S_QFA_YOYOP'].shift(-1))
AfterTurn['S_QFA_CGROP']=AfterTurn['S_QFA_CGROP'].where(~s,AfterTurn['S_QFA_CGROP'].shift(-1))
AfterTurn['S_QFA_YOYPROFIT']=AfterTurn['S_QFA_YOYPROFIT'].where(~s,AfterTurn['S_QFA_YOYPROFIT'].shift(-1))
AfterTurn['S_QFA_CGRPROFIT']=AfterTurn['S_QFA_CGRPROFIT'].where(~s,AfterTurn['S_QFA_CGRPROFIT'].shift(-1))
AfterTurn['S_QFA_YOYSALES']=AfterTurn['S_QFA_YOYSALES'].where(~s,AfterTurn['S_QFA_YOYSALES'].shift(-1))
AfterTurn['S_QFA_CGRSALES']=AfterTurn['S_QFA_CGRSALES'].where(~s,AfterTurn['S_QFA_CGRSALES'].shift(-1))



s_qfa_yoyop=AfterTurn[['S_INFO_WINDCODE','ANN_DT','S_QFA_YOYOP']].rename(columns={'ANN_DT':'TRADE_DT'})
s_qfa_yoyop_merge=pd.merge(TradeDate,s_qfa_yoyop,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')

s_qfa_yoyop_merge=s_qfa_yoyop_merge.drop_duplicates(subset=['TRADE_DT','S_INFO_WINDCODE'],keep='last').sort_values(by=['S_INFO_WINDCODE','TRADE_DT']).set_index(["TRADE_DT","S_INFO_WINDCODE"])
YOYOP_FILL=s_qfa_yoyop_merge['S_QFA_YOYOP'].unstack()
YOYOP_FILL_c=YOYOP_FILL.fillna(method='ffill')

final_yoyop=pd.merge(YOYOP_FILL_c.stack().reset_index(),TradeDate,on=['S_INFO_WINDCODE','TRADE_DT'],how='right')
final_yoyop=final_yoyop.rename(columns={0:'yoyop'})
final_yoyop["TRADE_DT"] = pd.to_datetime(final_yoyop["TRADE_DT"])
final_yoyop=final_yoyop.set_index(["TRADE_DT","S_INFO_WINDCODE"])


factor_init = final_yoyop["yoyop"].copy()
price_df = final_yoyop["S_DQ_CLOSE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)
al.tears.create_returns_tear_sheet(factor)



s_qfa_cgrop=AfterTurn[['S_INFO_WINDCODE','ANN_DT','S_QFA_CGROP']].rename(columns={'ANN_DT':'TRADE_DT'})
s_qfa_cgrop_merge=pd.merge(TradeDate,s_qfa_cgrop,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')

s_qfa_cgrop_merge=s_qfa_cgrop_merge.drop_duplicates(subset=['TRADE_DT','S_INFO_WINDCODE'],keep='last').sort_values(by=['S_INFO_WINDCODE','TRADE_DT']).set_index(["TRADE_DT","S_INFO_WINDCODE"])
CGROP_FILL=s_qfa_cgrop_merge['S_QFA_CGROP'].unstack()
CGROP_FILL_c=CGROP_FILL.fillna(method='ffill')

final_cgrop=pd.merge(CGROP_FILL_c.stack().reset_index(),TradeDate,on=['S_INFO_WINDCODE','TRADE_DT'],how='right')
final_cgrop=final_cgrop.rename(columns={0:'cgrop'})
final_cgrop["TRADE_DT"] = pd.to_datetime(final_cgrop["TRADE_DT"])
final_cgrop=final_cgrop.set_index(["TRADE_DT","S_INFO_WINDCODE"])


factor_init = final_cgrop["cgrop"].copy()
price_df = final_cgrop["S_DQ_CLOSE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)
al.tears.create_returns_tear_sheet(factor)


s_qfa_yoyprofit=AfterTurn[['S_INFO_WINDCODE','ANN_DT','S_QFA_YOYPROFIT']].rename(columns={'ANN_DT':'TRADE_DT'})
s_qfa_yoyprofit_merge=pd.merge(TradeDate,s_qfa_yoyprofit,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')

s_qfa_yoyprofit_merge=s_qfa_yoyprofit_merge.drop_duplicates(subset=['TRADE_DT','S_INFO_WINDCODE'],keep='last').sort_values(by=['S_INFO_WINDCODE','TRADE_DT']).set_index(["TRADE_DT","S_INFO_WINDCODE"])
YOYPROFIT_FILL=s_qfa_yoyprofit_merge['S_QFA_YOYPROFIT'].unstack()
YOYPROFIT_FILL_c=YOYPROFIT_FILL.fillna(method='ffill')

final_yoyprofit=pd.merge(YOYPROFIT_FILL_c.stack().reset_index(),TradeDate,on=['S_INFO_WINDCODE','TRADE_DT'],how='right')
final_yoyprofit=final_yoyprofit.rename(columns={0:'yoyprofit'})
final_yoyprofit["TRADE_DT"] = pd.to_datetime(final_yoyprofit["TRADE_DT"])
final_yoyprofit=final_yoyprofit.set_index(["TRADE_DT","S_INFO_WINDCODE"])


factor_init = final_yoyprofit["yoyprofit"].copy()
price_df = final_yoyprofit["S_DQ_CLOSE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)
al.tears.create_returns_tear_sheet(factor)




s_qfa_cgrprofit=AfterTurn[['S_INFO_WINDCODE','ANN_DT','S_QFA_CGRPROFIT']].rename(columns={'ANN_DT':'TRADE_DT'})
s_qfa_cgrprofit_merge=pd.merge(TradeDate,s_qfa_cgrprofit,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')

s_qfa_cgrprofit_merge=s_qfa_cgrprofit_merge.drop_duplicates(subset=['TRADE_DT','S_INFO_WINDCODE'],keep='last').sort_values(by=['S_INFO_WINDCODE','TRADE_DT']).set_index(["TRADE_DT","S_INFO_WINDCODE"])
CGRPROFIT_FILL=s_qfa_cgrprofit_merge['S_QFA_CGRPROFIT'].unstack()
CGRPROFIT_FILL_c=CGRPROFIT_FILL.fillna(method='ffill')

final_cgrprofit=pd.merge(CGRPROFIT_FILL_c.stack().reset_index(),TradeDate,on=['S_INFO_WINDCODE','TRADE_DT'],how='right')
final_cgrprofit=final_cgrprofit.rename(columns={0:'cgrprofit'})
final_cgrprofit["TRADE_DT"] = pd.to_datetime(final_cgrprofit["TRADE_DT"])
final_cgrprofit=final_cgrprofit.set_index(["TRADE_DT","S_INFO_WINDCODE"])


factor_init = final_cgrprofit["cgrprofit"].copy()
price_df = final_cgrprofit["S_DQ_CLOSE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)
al.tears.create_returns_tear_sheet(factor)


s_qfa_yoysales=AfterTurn[['S_INFO_WINDCODE','ANN_DT','S_QFA_YOYSALES']].rename(columns={'ANN_DT':'TRADE_DT'})
s_qfa_yoysales_merge=pd.merge(TradeDate,s_qfa_yoysales,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')

s_qfa_yoysales_merge=s_qfa_yoysales_merge.drop_duplicates(subset=['TRADE_DT','S_INFO_WINDCODE'],keep='last').sort_values(by=['S_INFO_WINDCODE','TRADE_DT']).set_index(["TRADE_DT","S_INFO_WINDCODE"])
YOYSALES_FILL=s_qfa_yoysales_merge['S_QFA_YOYSALES'].unstack()
YOYSALES_FILL_c=YOYSALES_FILL.fillna(method='ffill')

final_yoysales=pd.merge(YOYSALES_FILL_c.stack().reset_index(),TradeDate,on=['S_INFO_WINDCODE','TRADE_DT'],how='right')
final_yoysales=final_yoysales.rename(columns={0:'yoysales'})
final_yoysales["TRADE_DT"] = pd.to_datetime(final_yoysales["TRADE_DT"])
final_yoysales=final_yoysales.set_index(["TRADE_DT","S_INFO_WINDCODE"])


factor_init = final_yoysales["yoysales"].copy()
price_df = final_yoysales["S_DQ_CLOSE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)
al.tears.create_returns_tear_sheet(factor)



