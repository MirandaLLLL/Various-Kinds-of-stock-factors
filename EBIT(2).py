temp1["REPORT_PERIOD"] = pd.to_datetime(temp1["REPORT_PERIOD"])
temp1["ANN_DT"] = pd.to_datetime(temp1["ANN_DT"])
temp1 = temp1.drop_duplicates(subset=['REPORT_PERIOD', 'S_INFO_WINDCODE'], keep='last')

temp1 = temp1.set_index(["REPORT_PERIOD","S_INFO_WINDCODE"]).sort_values(by=['REPORT_PERIOD','S_INFO_WINDCODE'])
ebit_df = temp1["EBIT"].unstack().copy()
    # 将第一季度置为标准,后面二，三，四季度相减得增量
m = ebit_df.index.get_level_values(level='REPORT_PERIOD').quarter == 1
    # 每行元素置相同
mm = np.repeat(m, ebit_df.shape[1], axis=None).reshape(ebit_df.shape[0], ebit_df.shape[1])
ebit_diff = ebit_df.where(mm, ebit_df.diff())
ebit_new = pd.DataFrame(ebit_diff).stack().copy()
ebit_new = pd.merge(ebit_new.reset_index(),temp1.reset_index())

ebit_new=ebit_new.sort_values(by=['S_INFO_WINDCODE','REPORT_PERIOD'])
s=(ebit_new.S_INFO_WINDCODE!=ebit_new.shift(-1).S_INFO_WINDCODE)|((ebit_new.S_INFO_WINDCODE==ebit_new.shift(-1).S_INFO_WINDCODE)&(ebit_new.ANN_DT<=ebit_new.shift(-1).ANN_DT))
ebit_new[0]=ebit_new[0].where(s,ebit_new[0].shift(-1))
ebit_new=ebit_new[['ANN_DT','S_INFO_WINDCODE',0]]
ebit_new = pd.DataFrame(ebit_new).reset_index().rename(columns={'ANN_DT': 'TradingDay','S_INFO_WINDCODE':'SecuCode',0:'ebit'})

ebit_new = ebit_new.drop_duplicates(subset=['TradingDay', 'SecuCode'], keep='last')
ebit_new = ebit_new.set_index(["TradingDay", "SecuCode"])
ebit_new=ebit_new['ebit'].unstack(fill_value=0).replace(to_replace=0,
        method='ffill').asfreq('D', method='ffill')

ebit_new=ebit_new.stack().reset_index()
#ebit_new.to_csv('c:/ebit_new.csv')
merge_bench = pd.read_csv("C:/Users/merged_df_500.csv")
merge_bench["TradingDay"] = pd.to_datetime(merge_bench["TradingDay"])

factor_merge = pd.merge(merge_bench,ebit_new,how='left')
factor_merge['ebit2mv'] = factor_merge[0] / factor_merge['S_VAL_MV']
factor_merge_limit = factor_merge[['TradingDay', 'SecuCode', 0, 'S_VAL_MV', 'ebit2mv', 'close']]
    # factor_merge_limit.to_csv('c:/factor_merge_limit.csv')

factor_merge = factor_merge.rename(columns={'TradingDay': 'date', 'SecuCode': 'asset'})
factor_merge["date"] = pd.to_datetime(factor_merge["date"])
factor_merge = factor_merge.copy().set_index(["date", "asset"])
factor_initpb = factor_merge["b2p"].copy()
factor_initturn = factor_merge["TURN"].copy()
factor_size = factor_merge['ebit2mv'].copy()

price_df2 = factor_merge["close"].unstack().copy()


factor = al.utils.get_clean_factor_and_forward_returns(factor_size, price_df2, max_loss=1)
al.tears.create_returns_tear_sheet(factor)
al.tears.create_returns_tear_sheet2(factor, factor_initturn, factor_initpb)

