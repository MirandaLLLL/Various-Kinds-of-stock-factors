import pandas as pd
import cx_Oracle
import calendar
import alphalens as al
import numpy as np


wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_VAL_PE_TTM,S_DQ_CLOSE_TODAY from AShareEODDerivativeIndicator  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20100930'
 '''
PE_table = pd.read_sql(sqlcmd, con=wd)




wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20100930'
 '''
TradeDate = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,REPORT_PERIOD,STATEMENT_TYPE,NET_PROFIT_INCL_MIN_INT_INC,NET_PROFIT_EXCL_MIN_INT_INC from AShareIncome  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and REPORT_PERIOD>'20100929'and STATEMENT_TYPE='408005000' 
 '''
Net_Profit_bc = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,REPORT_PERIOD,ANN_DT,STATEMENT_TYPE,NET_PROFIT_INCL_MIN_INT_INC,NET_PROFIT_EXCL_MIN_INT_INC from AShareIncome  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and REPORT_PERIOD>'20100930'and STATEMENT_TYPE='408001000' 
 '''
Net_Profit_COMBINE = pd.read_sql(sqlcmd, con=wd)

Net_Profit_COMBINE["REPORT_PERIOD"] = pd.to_datetime(Net_Profit_COMBINE["REPORT_PERIOD"])
Net_Profit_COMBINE["ANN_DT"] = pd.to_datetime(Net_Profit_COMBINE["ANN_DT"])
Net_Profit_bc["REPORT_PERIOD"] = pd.to_datetime(Net_Profit_bc["REPORT_PERIOD"])
PE_table["TRADE_DT"] = pd.to_datetime(PE_table["TRADE_DT"])
TradeDate["TRADE_DT"] = pd.to_datetime(TradeDate["TRADE_DT"])

merge_NET_PROFIT=pd.merge(Net_Profit_COMBINE,Net_Profit_bc,on=["S_INFO_WINDCODE","REPORT_PERIOD"],how="outer")
merge_NET_PROFIT=merge_NET_PROFIT.drop(columns=['STATEMENT_TYPE_y','STATEMENT_TYPE_x'])
merge_NET_PROFIT=merge_NET_PROFIT.sort_values(by=["S_INFO_WINDCODE","REPORT_PERIOD"])

condition=~(merge_NET_PROFIT.isnull())
merge_NET_PROFIT_copy=merge_NET_PROFIT.copy()
merge_NET_PROFIT_copy=merge_NET_PROFIT_copy.where(condition,merge_NET_PROFIT.shift(periods=2, axis=1))

Ori_Net=merge_NET_PROFIT_copy[["S_INFO_WINDCODE","REPORT_PERIOD","ANN_DT","NET_PROFIT_INCL_MIN_INT_INC_y","NET_PROFIT_EXCL_MIN_INT_INC_y"]]

Ori_Net=Ori_Net.sort_values(["S_INFO_WINDCODE","REPORT_PERIOD"]).set_index(["REPORT_PERIOD","S_INFO_WINDCODE"])

PerSea_NetP_in=Ori_Net['NET_PROFIT_INCL_MIN_INT_INC_y'].unstack()
m = PerSea_NetP_in.index.get_level_values(level='REPORT_PERIOD').quarter == 1
    # 每行元素置相同
mm = np.repeat(m, PerSea_NetP_in.shape[1], axis=None).reshape(PerSea_NetP_in.shape[0], PerSea_NetP_in.shape[1])
PerSea_NetP_in_c = PerSea_NetP_in.where(mm, PerSea_NetP_in.diff())

Net_in=PerSea_NetP_in_c.stack().reset_index()
Net_in_final=pd.merge(Net_in,Ori_Net.reset_index())
Net_in_final=Net_in_final.rename(columns={0:"Per_netP_in"})
Net_in_final=Net_in_final.sort_values(by=["S_INFO_WINDCODE","REPORT_PERIOD"])

Net_in_final_growth=Net_in_final.copy().set_index(["REPORT_PERIOD","S_INFO_WINDCODE"])
Net_in_final_growth=Net_in_final_growth['Per_netP_in'].unstack()
Net_in_final_growth=(Net_in_final_growth-Net_in_final_growth.shift(4))/abs(Net_in_final_growth.shift(4))

Rate_in=Net_in_final_growth.stack().reset_index().sort_values(by=['S_INFO_WINDCODE','REPORT_PERIOD'])
Rate_in_combine=pd.merge(Rate_in,Net_in_final,on=['REPORT_PERIOD','S_INFO_WINDCODE'])
Rate_in_combine=Rate_in_combine.sort_values(by=['S_INFO_WINDCODE','REPORT_PERIOD'])
s=(Rate_in_combine.S_INFO_WINDCODE==Rate_in_combine.shift(-1).S_INFO_WINDCODE)&(Rate_in_combine.ANN_DT>Rate_in_combine.shift(-1).ANN_DT)
AfterTurn=Rate_in_combine.copy()
AfterTurn[0]=AfterTurn[0].where(~s,AfterTurn[0].shift(-1))

PE_table=PE_table.sort_values(by=["S_INFO_WINDCODE","TRADE_DT"])

IN_netP_final=AfterTurn[["S_INFO_WINDCODE","ANN_DT",0]].rename(columns={'ANN_DT':'TRADE_DT',0:'rate_in'})

PEG=pd.merge(IN_netP_final,PE_table,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')

PEG_Original=PEG.copy()

PEG_Original=PEG_Original.sort_values(by=["S_INFO_WINDCODE","TRADE_DT"])

PEG_Original=PEG_Original.drop_duplicates(subset=['TRADE_DT','S_INFO_WINDCODE'],keep='last')

PEG_Original=PEG_Original.set_index(["TRADE_DT","S_INFO_WINDCODE"])

PEG_FILL=PEG_Original['rate_in'].unstack()

PEG_FILL=PEG_FILL.fillna(method='ffill')

PEG_AfterFill=pd.merge(PEG_FILL.stack().reset_index(),PE_table,on=["S_INFO_WINDCODE","TRADE_DT"],how='outer')

PEG_AfterFill['PEG']=PEG_AfterFill['S_VAL_PE_TTM']/PEG_AfterFill[0]

FINAL=pd.merge(PEG_AfterFill,TradeDate,on=["S_INFO_WINDCODE","TRADE_DT"],how='right')

FINAL_test=FINAL[['S_INFO_WINDCODE','TRADE_DT','PEG','S_DQ_CLOSE']]

FINAL_test=FINAL_test.set_index(["TRADE_DT","S_INFO_WINDCODE"])

PEG_AfterFill=PEG_AfterFill.rename(columns={0:'growth'})

PEG_cr= PEG_AfterFill.drop(PEG_AfterFill[(PEG_AfterFill.growth<0)&(PEG_AfterFill.S_VAL_PE_TTM <0)].index)

PEG_cr['G/PE']=PEG_cr['growth']/PEG_cr['S_VAL_PE_TTM']

FINAL=pd.merge(PEG_cr,TradeDate,on=["S_INFO_WINDCODE","TRADE_DT"],how='right')
FINAL_test=FINAL[['S_INFO_WINDCODE','TRADE_DT','G/PE','S_DQ_CLOSE']]
FINAL_test=FINAL_test.set_index(['TRADE_DT','S_INFO_WINDCODE'])
factor_init = FINAL_test["G/PE"].copy()
price_df = FINAL_test["S_DQ_CLOSE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)

al.tears.create_returns_tear_sheet(factor)

