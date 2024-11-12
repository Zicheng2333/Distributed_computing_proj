import pandas as pd

order_file_path = '20190102/am_hq_order_spot.txt'
trade_file_path = '20190102/am_hq_trade_spot.txt'
#trade_file_path = '20190102/test.txt'

order_data = pd.read_csv(order_file_path,sep='\t',header=None, dtype={14:str, 15:str})
trade_data = pd.read_csv(trade_file_path,sep='\t',header=None,dtype={15:str})

print('len trade data:', len(trade_data))

Security_ID = '000016' #TODO 选择一只股票
time_window = ['20190102091500000','20190102113000073']

order_col = ['tradedate', 'OrigTime', 'SendTime', 'recvtime',
'dbtime','ChannelNo', 'MDStreamID', 'ApplSeqNum', 'SecurityID',
'SecurityIDSource','Price', 'OrderQty', 'TransactTime', 'Side',
'OrderType', 'ConfirmID','Contactor', 'ContactInfo',
       'ExpirationDays', 'ExpirationType']

trade_col = [
    "tradedate", "OrigTime", "SendTime", "recvtime", "dbtime",
    "ChannelNo", "MDStreamID", "ApplSeqNum", "SecurityID", "SecurityIDSource",
    "BidApplSeqNum", "OfferApplSeqNum", "Price", "TradeQty", "ExecType",
    "tradetime"
]

order_data.columns = order_col
trade_data.columns = trade_col
time1, time2 = int(time_window[0]), int(time_window[1])

#TODO step1: 遍历成交数据，只选取 exec_type=F, security_id正确, 位于时间窗口之内的数据
condition = (
    (trade_data['ExecType'] == 'F') &
    (trade_data['SecurityID'] == int(Security_ID)) &
    (trade_data['tradetime'].astype(int).between(time1, time2))
)

"""# 访问第一行数据
row = trade_data.iloc[0]
# 检查条件是否满足
print(row['ExecType'] == 'F')  # 检查 ExecType
print(row['SecurityID'] == int(Security_ID))  # 检查 SecurityID
print(int(row['tradetime']) >= time1 and int(row['tradetime']) <= time2)  # 检查时间窗口"""

filtered_data = trade_data[condition].copy()
print('col: ',filtered_data.columns)
print('len: ',len(filtered_data))

#TODO step2: 在逐笔委托中查询该单涉及的买卖委托
filtered_data['type'] = None
for index, row in filtered_data.iterrows():
    OfferApplSeqNum = row['OfferApplSeqNum']
    BidApplSeqNum = row['BidApplSeqNum']

    Offer_time = order_data[order_data['ApplSeqNum'] == OfferApplSeqNum]['TransactTime'] #是否会有多个时间？？？？
    Bid_time = order_data[order_data['ApplSeqNum'] == BidApplSeqNum]['TransactTime']

    if int(Offer_time.iloc[0])>int(Bid_time.iloc[0]):
        filtered_data.at[index, 'type'] = 0
    else:
        filtered_data.at[index, 'type'] = 1

filtered_data.to_csv('filtered_data.txt', index=False, sep=' ', header=False)






