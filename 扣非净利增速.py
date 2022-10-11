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

data_use_deducted=Factor_Base[['S_INFO_WINDCODE','REPORT_PERIOD','ANN_DT','S_FA_DEDUCTEDPROFIT']]

data_use_deducted=data_use_deducted.sort_values(by=['S_INFO_WINDCODE','REPORT_PERIOD'])

data_use_deducted["REPORT_PERIOD"]=pd.to_datetime(data_use_deducted["REPORT_PERIOD"])
data_use_deducted["ANN_DT"]=pd.to_datetime(data_use_deducted["ANN_DT"])
data_use_deducteda_copy=data_use_deducted.copy()
data_use_deducted=data_use_deducted.set_index(["REPORT_PERIOD","S_INFO_WINDCODE"])


deduct_un = data_use_deducted['S_FA_DEDUCTEDPROFIT'].unstack().copy()
    # 将第一季度置为标准,后面二，三，四季度相减得增量
m = deduct_un.index.get_level_values(level='REPORT_PERIOD').quarter == 1
    # 每行元素置相同
mm = np.repeat(m, deduct_un.shape[1], axis=None).reshape(deduct_un.shape[0], deduct_un.shape[1])
deduct_diff = deduct_un.where(mm, deduct_un.diff())

per_deduct=deduct_diff.stack().reset_index()
per_deduct_table=pd.merge(per_deduct,data_use_deducteda_copy,on=['S_INFO_WINDCODE','REPORT_PERIOD'],how='outer')

per_deduct_table=per_deduct_table.rename(columns={0:'per_deduct'})

per_rate=per_deduct_table.set_index(["REPORT_PERIOD","S_INFO_WINDCODE"])


per_rate_un=per_rate['per_deduct'].unstack()
per_rate_c=(per_rate_un-per_rate_un.shift(4))/abs(per_rate_un.shift(4))

per_rate_re=per_rate_c.stack().reset_index()
per_rate_table=pd.merge(per_rate_re,per_deduct_table,on=['REPORT_PERIOD','S_INFO_WINDCODE'],how='outer')

per_rate_table=per_rate_table.rename(columns={0:'per_rate'}).sort_values(by=['S_INFO_WINDCODE','REPORT_PERIOD'])

s=(per_rate_table.S_INFO_WINDCODE==per_rate_table.shift(-1).S_INFO_WINDCODE)&(per_rate_table.ANN_DT>per_rate_table.shift(-1).ANN_DT)
AfterTurn=per_rate_table.copy()
AfterTurn['per_rate']=AfterTurn['per_rate'].where(~s,AfterTurn['per_rate'].shift(-1))

per_merge_1=per_rate_table[['S_INFO_WINDCODE','ANN_DT','per_rate']]
per_merge_1=per_merge_1.rename(columns={'ANN_DT':'TRADE_DT'})

per_merge_1['TRADE_DT']=pd.to_datetime(per_merge_1['TRADE_DT'])
TradeDate['TRADE_DT']=pd.to_datetime(TradeDate['TRADE_DT'])
per_merge_2=pd.merge(per_merge_1,TradeDate,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
per_merge_3=per_merge_2.drop_duplicates(subset=['TRADE_DT','S_INFO_WINDCODE'],keep='last').sort_values(by=['S_INFO_WINDCODE','TRADE_DT']).set_index(["TRADE_DT","S_INFO_WINDCODE"])
per_merge_fill=per_merge_3['per_rate'].unstack()
per_merge_fill_c=per_merge_fill.fillna(method='ffill')
final=pd.merge(per_merge_fill_c.stack().reset_index(),TradeDate,on=['S_INFO_WINDCODE','TRADE_DT'],how='right')


final=final.rename(columns={0:'DeductRate'}).sort_values(by=['S_INFO_WINDCODE','TRADE_DT']).set_index(["TRADE_DT","S_INFO_WINDCODE"])

factor_init = final["DeductRate"].copy()
price_df = final["S_DQ_CLOSE"].unstack().copy()
factor = al.utils.get_clean_factor_and_forward_returns(factor_init, price_df,max_loss=0.51)
al.tears.create_returns_tear_sheet(factor)
