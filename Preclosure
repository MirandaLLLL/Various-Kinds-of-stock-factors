import pandas as pd
import numpy as np
import alphalens as al
import cx_Oracle

Reservation2019_Q3=pd.read_csv('D:/szsh2019-09-30.csv',encoding='unicode_escape')
Reservation2019_Q4=pd.read_csv('D:/szsh2019-12-31.csv',encoding='unicode_escape')
Reservation2020_Q1=pd.read_csv('D:/szsh2020-03-31.csv',encoding='unicode_escape')
Reservation2020_Q2=pd.read_csv('D:/szsh2020-06-30.csv',encoding='unicode_escape')

Publish2019_Q3=Reservation2019_Q3[['S_INFO_WINDCODE','publishDate0']]
Publish2019_Q3['S_INFO_WINDCODE']=Publish2019_Q3['S_INFO_WINDCODE'].astype(str).str.zfill(6)
Publish2019_Q3['publishDate0']=pd.to_datetime(Publish2019_Q3['publishDate0'])
Publish2019_Q3=Publish2019_Q3.sort_values(by=['S_INFO_WINDCODE']).drop_duplicates(subset={'S_INFO_WINDCODE'},keep='last')
Publish2019_Q3['Publish_Date']='2019-09-26'
Publish2019_Q3['Publish_Date']=pd.to_datetime(Publish2019_Q3['Publish_Date'])
Publish2019_Q3=Publish2019_Q3.rename(columns={'Publish_Date':'TRADE_DT'})

Publish2019_Q4=Reservation2019_Q4[['S_INFO_WINDCODE','publishDate0']]
Publish2019_Q4['S_INFO_WINDCODE']=Publish2019_Q4['S_INFO_WINDCODE'].astype(str).str.zfill(6)
Publish2019_Q4['publishDate0']=pd.to_datetime(Publish2019_Q4['publishDate0'])
Publish2019_Q4=Publish2019_Q4.sort_values(by=['S_INFO_WINDCODE']).drop_duplicates(subset={'S_INFO_WINDCODE'},keep='last')
Publish2019_Q4['Publish_Date']='2019-12-27'
Publish2019_Q4['Publish_Date']=pd.to_datetime(Publish2019_Q4['Publish_Date'])
Publish2019_Q4=Publish2019_Q4.rename(columns={'Publish_Date':'TRADE_DT'})

Publish2020_Q1=Reservation2020_Q1[['S_INFO_WINDCODE','publishDate0']]
Publish2020_Q1['S_INFO_WINDCODE']=Publish2020_Q1['S_INFO_WINDCODE'].astype(str).str.zfill(6)
Publish2020_Q1['publishDate0']=pd.to_datetime(Publish2020_Q1['publishDate0'])
Publish2020_Q1=Publish2020_Q1.sort_values(by=['S_INFO_WINDCODE']).drop_duplicates(subset={'S_INFO_WINDCODE'},keep='last')
Publish2020_Q1['Publish_Date']='2020-03-26'
Publish2020_Q1['Publish_Date']=pd.to_datetime(Publish2020_Q1['Publish_Date'])
Publish2020_Q1=Publish2020_Q1.rename(columns={'Publish_Date':'TRADE_DT'})

Publish2020_Q2=Reservation2020_Q2[['S_INFO_WINDCODE','publishDate0']]
Publish2020_Q2['S_INFO_WINDCODE']=Publish2020_Q2['S_INFO_WINDCODE'].astype(str).str.zfill(6)
Publish2020_Q2['publishDate0']=pd.to_datetime(Publish2020_Q2['publishDate0'])
Publish2020_Q2=Publish2020_Q2.sort_values(by=['S_INFO_WINDCODE']).drop_duplicates(subset={'S_INFO_WINDCODE'},keep='last')
Publish2020_Q2['Publish_Date']='2020-06-26'
Publish2020_Q2['Publish_Date']=pd.to_datetime(Publish2020_Q2['Publish_Date'])
Publish2020_Q2=Publish2020_Q2.rename(columns={'Publish_Date':'TRADE_DT'})

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20190925'and TRADE_DT<'201901009'
 '''
TradeDate_2019_Q3_1D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20190925'and TRADE_DT<'20191017'
 '''
TradeDate_2019_Q3_5D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20190925'and TRADE_DT<'20191024'
 '''
TradeDate_2019_Q3_10D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20190925'and TRADE_DT<'20191031'
 '''
TradeDate_2019_Q3_15D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20190925'and TRADE_DT<'20191107'
 '''
TradeDate_2019_Q3_20D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20190925'and TRADE_DT<'20191121'
 '''
TradeDate_2019_Q3_30D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20191226'and TRADE_DT<'20191231'
 '''
TradeDate_2019_Q4_1D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20191226'and TRADE_DT<'20200107'
 '''
TradeDate_2019_Q4_5D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20191226'and TRADE_DT<'20200114'
 '''
TradeDate_2019_Q4_10D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20191226'and TRADE_DT<'20200121'
 '''
TradeDate_2019_Q4_15D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20191226'and TRADE_DT<'20200128'
 '''
TradeDate_2019_Q4_20D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20191226'and TRADE_DT<'20200211'
 '''
TradeDate_2019_Q4_30D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20200325'and TRADE_DT<'20200331'
 '''
TradeDate_2020_Q1_1D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20200325'and TRADE_DT<'20200408'
 '''
TradeDate_2020_Q1_5D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20200325'and TRADE_DT<'20200415'
 '''
TradeDate_2020_Q1_10D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20200325'and TRADE_DT<'20200422'
 '''
TradeDate_2020_Q1_15D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20200325'and TRADE_DT<'20200430'
 '''
TradeDate_2020_Q1_20D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20200325'and TRADE_DT<'20200508'
 '''
TradeDate_2020_Q1_30D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20200625'and TRADE_DT<'20200630'
 '''
TradeDate_2020_Q2_1D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20200625'and TRADE_DT<'20200630'
 '''
TradeDate_2020_Q2_5D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20200625'and TRADE_DT<'20200708'
 '''
TradeDate_2020_Q2_10D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20200625'and TRADE_DT<'20200715'
 '''
TradeDate_2020_Q2_15D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20200625'and TRADE_DT<'20200722'
 '''
TradeDate_2020_Q2_20D = pd.read_sql(sqlcmd, con=wd)

wd = cx_Oracle.connect('wind/wind1234@10.35.20.123:1521/winddb')
sqlcmd = '''select S_INFO_WINDCODE,TRADE_DT,S_DQ_CLOSE from AShareEODPrices  where S_INFO_WINDCODE 
in(select S_CON_WINDCODE FROM AINDEXMEMBERS WHERE S_INFO_WINDCODE = '000905.SH')and TRADE_DT>'20200625'and TRADE_DT<'20200807'
 '''
TradeDate_2020_Q2_30D = pd.read_sql(sqlcmd, con=wd)


TradeDate_2019_Q3_1D['S_INFO_WINDCODE']=TradeDate_2019_Q3_1D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])
TradeDate_2019_Q3_5D['S_INFO_WINDCODE']=TradeDate_2019_Q3_5D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])
TradeDate_2019_Q3_10D['S_INFO_WINDCODE']=TradeDate_2019_Q3_10D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])
TradeDate_2019_Q3_15D['S_INFO_WINDCODE']=TradeDate_2019_Q3_15D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])
TradeDate_2019_Q3_20D['S_INFO_WINDCODE']=TradeDate_2019_Q3_20D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])
TradeDate_2019_Q3_30D['S_INFO_WINDCODE']=TradeDate_2019_Q3_30D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])

TradeDate_2019_Q4_1D['S_INFO_WINDCODE']=TradeDate_2019_Q4_1D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])
TradeDate_2019_Q4_5D['S_INFO_WINDCODE']=TradeDate_2019_Q4_5D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])
TradeDate_2019_Q4_10D['S_INFO_WINDCODE']=TradeDate_2019_Q4_10D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])
TradeDate_2019_Q4_15D['S_INFO_WINDCODE']=TradeDate_2019_Q4_15D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])
TradeDate_2019_Q4_20D['S_INFO_WINDCODE']=TradeDate_2019_Q4_20D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])
TradeDate_2019_Q4_30D['S_INFO_WINDCODE']=TradeDate_2019_Q4_30D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])

TradeDate_2020_Q1_1D['S_INFO_WINDCODE']=TradeDate_2020_Q1_1D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])
TradeDate_2020_Q1_5D['S_INFO_WINDCODE']=TradeDate_2020_Q1_5D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])
TradeDate_2020_Q1_10D['S_INFO_WINDCODE']=TradeDate_2020_Q1_10D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])
TradeDate_2020_Q1_15D['S_INFO_WINDCODE']=TradeDate_2020_Q1_15D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])
TradeDate_2020_Q1_20D['S_INFO_WINDCODE']=TradeDate_2020_Q1_20D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])
TradeDate_2020_Q1_30D['S_INFO_WINDCODE']=TradeDate_2020_Q1_30D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])

TradeDate_2020_Q2_1D['S_INFO_WINDCODE']=TradeDate_2020_Q2_1D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])
TradeDate_2020_Q2_5D['S_INFO_WINDCODE']=TradeDate_2020_Q2_5D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])
TradeDate_2020_Q2_10D['S_INFO_WINDCODE']=TradeDate_2020_Q2_10D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])
TradeDate_2020_Q2_15D['S_INFO_WINDCODE']=TradeDate_2020_Q2_15D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])
TradeDate_2020_Q2_20D['S_INFO_WINDCODE']=TradeDate_2020_Q2_20D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])
TradeDate_2020_Q2_30D['S_INFO_WINDCODE']=TradeDate_2020_Q2_30D['S_INFO_WINDCODE'].apply(lambda x:x[:-3])

TradeDate_2019_Q3_1D['TRADE_DT']=pd.to_datetime(TradeDate_2019_Q3_1D['TRADE_DT'])
TradeDate_2019_Q3_5D['TRADE_DT']=pd.to_datetime(TradeDate_2019_Q3_5D['TRADE_DT'])
TradeDate_2019_Q3_10D['TRADE_DT']=pd.to_datetime(TradeDate_2019_Q3_10D['TRADE_DT'])
TradeDate_2019_Q3_15D['TRADE_DT']=pd.to_datetime(TradeDate_2019_Q3_15D['TRADE_DT'])
TradeDate_2019_Q3_20D['TRADE_DT']=pd.to_datetime(TradeDate_2019_Q3_20D['TRADE_DT'])
TradeDate_2019_Q3_30D['TRADE_DT']=pd.to_datetime(TradeDate_2019_Q3_30D['TRADE_DT'])

TradeDate_2019_Q4_1D['TRADE_DT']=pd.to_datetime(TradeDate_2019_Q4_1D['TRADE_DT'])
TradeDate_2019_Q4_5D['TRADE_DT']=pd.to_datetime(TradeDate_2019_Q4_5D['TRADE_DT'])
TradeDate_2019_Q4_10D['TRADE_DT']=pd.to_datetime(TradeDate_2019_Q4_10D['TRADE_DT'])
TradeDate_2019_Q4_15D['TRADE_DT']=pd.to_datetime(TradeDate_2019_Q4_15D['TRADE_DT'])
TradeDate_2019_Q4_20D['TRADE_DT']=pd.to_datetime(TradeDate_2019_Q4_20D['TRADE_DT'])
TradeDate_2019_Q4_30D['TRADE_DT']=pd.to_datetime(TradeDate_2019_Q4_30D['TRADE_DT'])

TradeDate_2020_Q1_1D['TRADE_DT']=pd.to_datetime(TradeDate_2020_Q1_1D['TRADE_DT'])
TradeDate_2020_Q1_5D['TRADE_DT']=pd.to_datetime(TradeDate_2020_Q1_5D['TRADE_DT'])
TradeDate_2020_Q1_10D['TRADE_DT']=pd.to_datetime(TradeDate_2020_Q1_10D['TRADE_DT'])
TradeDate_2020_Q1_15D['TRADE_DT']=pd.to_datetime(TradeDate_2020_Q1_15D['TRADE_DT'])
TradeDate_2020_Q1_20D['TRADE_DT']=pd.to_datetime(TradeDate_2020_Q1_20D['TRADE_DT'])
TradeDate_2020_Q1_30D['TRADE_DT']=pd.to_datetime(TradeDate_2020_Q1_30D['TRADE_DT'])

TradeDate_2020_Q2_1D['TRADE_DT']=pd.to_datetime(TradeDate_2020_Q2_1D['TRADE_DT'])
TradeDate_2020_Q2_5D['TRADE_DT']=pd.to_datetime(TradeDate_2020_Q2_5D['TRADE_DT'])
TradeDate_2020_Q2_10D['TRADE_DT']=pd.to_datetime(TradeDate_2020_Q2_10D['TRADE_DT'])
TradeDate_2020_Q2_15D['TRADE_DT']=pd.to_datetime(TradeDate_2020_Q2_15D['TRADE_DT'])
TradeDate_2020_Q2_20D['TRADE_DT']=pd.to_datetime(TradeDate_2020_Q2_20D['TRADE_DT'])
TradeDate_2020_Q2_30D['TRADE_DT']=pd.to_datetime(TradeDate_2020_Q2_30D['TRADE_DT'])

merge_2019_Q3_1d=pd.merge(TradeDate_2019_Q3_1D,Publish2019_Q3,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2019_Q3_1d=merge_2019_Q3_1d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2019_Q3_1d=merge_2019_Q3_1d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2019_Q3_5d=pd.merge(TradeDate_2019_Q3_5D,Publish2019_Q3,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2019_Q3_5d=merge_2019_Q3_5d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2019_Q3_5d=merge_2019_Q3_5d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2019_Q3_10d=pd.merge(TradeDate_2019_Q3_10D,Publish2019_Q3,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2019_Q3_10d=merge_2019_Q3_10d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2019_Q3_10d=merge_2019_Q3_10d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2019_Q3_15d=pd.merge(TradeDate_2019_Q3_15D,Publish2019_Q3,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2019_Q3_15d=merge_2019_Q3_15d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2019_Q3_15d=merge_2019_Q3_15d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2019_Q3_20d=pd.merge(TradeDate_2019_Q3_20D,Publish2019_Q3,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2019_Q3_20d=merge_2019_Q3_20d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2019_Q3_20d=merge_2019_Q3_20d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2019_Q3_30d=pd.merge(TradeDate_2019_Q3_30D,Publish2019_Q3,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2019_Q3_30d=merge_2019_Q3_30d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2019_Q3_30d=merge_2019_Q3_30d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2019_Q4_1d=pd.merge(TradeDate_2019_Q4_1D,Publish2019_Q4,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2019_Q4_1d=merge_2019_Q4_1d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2019_Q4_1d=merge_2019_Q4_1d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2019_Q4_5d=pd.merge(TradeDate_2019_Q4_5D,Publish2019_Q4,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2019_Q4_5d=merge_2019_Q4_5d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2019_Q4_5d=merge_2019_Q4_5d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2019_Q4_10d=pd.merge(TradeDate_2019_Q4_10D,Publish2019_Q4,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2019_Q4_10d=merge_2019_Q4_10d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2019_Q4_10d=merge_2019_Q4_10d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2019_Q4_15d=pd.merge(TradeDate_2019_Q4_15D,Publish2019_Q4,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2019_Q4_15d=merge_2019_Q4_15d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2019_Q4_15d=merge_2019_Q4_15d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2019_Q4_20d=pd.merge(TradeDate_2019_Q4_20D,Publish2019_Q4,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2019_Q4_20d=merge_2019_Q4_20d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2019_Q4_20d=merge_2019_Q4_20d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2019_Q4_30d=pd.merge(TradeDate_2019_Q4_30D,Publish2019_Q4,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2019_Q4_30d=merge_2019_Q4_30d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2019_Q4_30d=merge_2019_Q4_30d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2020_Q1_1d=pd.merge(TradeDate_2020_Q1_1D,Publish2020_Q1,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2020_Q1_1d=merge_2020_Q1_1d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2020_Q1_1d=merge_2020_Q1_1d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2020_Q1_5d=pd.merge(TradeDate_2020_Q1_5D,Publish2020_Q1,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2020_Q1_5d=merge_2020_Q1_5d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2020_Q1_5d=merge_2020_Q1_5d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2020_Q1_10d=pd.merge(TradeDate_2020_Q1_10D,Publish2020_Q1,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2020_Q1_10d=merge_2020_Q1_10d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2020_Q1_10d=merge_2020_Q1_10d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2020_Q1_15d=pd.merge(TradeDate_2020_Q1_15D,Publish2020_Q1,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2020_Q1_15d=merge_2020_Q1_15d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2020_Q1_15d=merge_2020_Q1_15d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2020_Q1_20d=pd.merge(TradeDate_2020_Q1_20D,Publish2020_Q1,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2020_Q1_20d=merge_2020_Q1_20d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2020_Q1_20d=merge_2020_Q1_20d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2020_Q1_30d=pd.merge(TradeDate_2020_Q1_30D,Publish2020_Q1,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2020_Q1_30d=merge_2020_Q1_30d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2020_Q1_30d=merge_2020_Q1_30d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2020_Q2_1d=pd.merge(TradeDate_2020_Q2_1D,Publish2020_Q2,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2020_Q2_1d=merge_2020_Q2_1d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2020_Q2_1d=merge_2020_Q2_1d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2020_Q2_5d=pd.merge(TradeDate_2020_Q2_5D,Publish2020_Q2,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2020_Q2_5d=merge_2020_Q2_5d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2020_Q2_5d=merge_2020_Q2_5d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2020_Q2_10d=pd.merge(TradeDate_2020_Q2_10D,Publish2020_Q2,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2020_Q2_10d=merge_2020_Q2_10d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2020_Q2_10d=merge_2020_Q2_10d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2020_Q2_15d=pd.merge(TradeDate_2020_Q2_15D,Publish2020_Q2,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2020_Q2_15d=merge_2020_Q2_15d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2020_Q2_15d=merge_2020_Q2_15d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2020_Q2_20d=pd.merge(TradeDate_2020_Q2_20D,Publish2020_Q2,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2020_Q2_20d=merge_2020_Q2_20d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2020_Q2_20d=merge_2020_Q2_20d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

merge_2020_Q2_30d=pd.merge(TradeDate_2020_Q2_30D,Publish2020_Q2,on=['S_INFO_WINDCODE','TRADE_DT'],how='outer')
merge_2020_Q2_30d=merge_2020_Q2_30d.dropna(axis=0,subset = ["S_DQ_CLOSE"])
merge_2020_Q2_30d=merge_2020_Q2_30d.sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])

fill_1=merge_2019_Q3_30d.set_index(["TRADE_DT","S_INFO_WINDCODE"])
fill_2=fill_1['publishDate0'].unstack()
fill_3=fill_2.fillna(method='ffill')
fill_table=pd.merge(fill_3.stack().reset_index(),merge_2019_Q3_30d,on=['TRADE_DT','S_INFO_WINDCODE']).rename(columns={0:'Publish_d'}).sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])
rank_1=fill_table.set_index(["S_INFO_WINDCODE","TRADE_DT"])
rank_2=rank_1['Publish_d'].unstack()
rank_3=rank_2.rank()
rank_table=pd.merge(rank_3.stack().reset_index(),fill_table,on=['TRADE_DT','S_INFO_WINDCODE']).rename(columns={0:'rank'}).sort_values(by=['S_INFO_WINDCODE','TRADE_DT'])
select_data=rank_table[(rank_table['rank']<10)]
final_1=select_data.set_index(["TRADE_DT","S_INFO_WINDCODE"])
final_2=final_1['S_DQ_CLOSE'].unstack()
final_1D=(final_2-final_2.shift(1))/final_2.shift(1)
final_5D=(final_2-final_2.shift(5))/final_2.shift(5)
final_10D=(final_2-final_2.shift(10))/final_2.shift(10)
final_15D=(final_2-final_2.shift(15))/final_2.shift(15)
final_20D=(final_2-final_2.shift(20))/final_2.shift(20)
final_30D=(final_2-final_2.shift(30))/final_2.shift(30)






