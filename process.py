import pandas as pd

order_file_path = '20190102/am_hq_order_spot.txt'
trade_file_path = '20190102/am_hq_trade_spot.txt'

order_data = pd.read_csv(order_file_path,sep='\s',header=None)
trade_data = pd.read_csv(trade_file_path,sep='\s',header=None)

Security_ID = '' #TODO 选择一只股票
time_window = ['','']

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
    (trade_data['SecurityID'] == Security_ID) &
    (trade_data['tradetime'].astype(int).between(time1, time2))
)

filtered_data = trade_data[condition]

#TODO step2: 在逐笔委托中查询该单涉及的买卖委托
for col in filtered_data:
    OfferApplSeqNum = col['OfferApplSeqNum']
    BidApplSeqNum = col['BidApplSeqNum']








