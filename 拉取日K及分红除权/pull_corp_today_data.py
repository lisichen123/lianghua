import tushare as ts
import pymysql
import time
from datetime import datetime, timedelta
from loguru import logger

# 设置 Tushare Token
api_key = 'ee2eaeec92dfabceee2464944162480a96266e64de1af4448a8378a3'
ts.set_token(api_key)
pro = ts.pro_api()

# 配置 MySQL 连接
conn = pymysql.connect(
    host='localhost',
    user='root',
    password='123456',
    database='stock_data',
    charset='utf8mb4'
)
cursor = conn.cursor()

today = datetime.today().date()
trade_date_str = today.strftime('%Y%m%d')
try:
    print(f'拉取数据中：{trade_date_str}')
    df = pro.daily(trade_date=trade_date_str)

    if df.empty:
        print(f'{trade_date_str} 不是交易日，跳过')

    for _, row in df.iterrows():
        sql = """
            INSERT IGNORE INTO stock_backward_adjusted (
                ts_code, trade_date, open, high, low, close,
                pre_close, change_amt, pct_chg, vol, amount
            ) VALUES (
                %s, STR_TO_DATE(%s, '%%Y%%m%%d'), %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
        """
        data = (
            row['ts_code'],
            row['trade_date'],
            row['open'],
            row['high'],
            row['low'],
            row['close'],
            row['pre_close'],
            row['change'],
            row['pct_chg'],
            row['vol'],
            row['amount']
        )
        cursor.execute(sql, data)
    conn.commit()
    print(f'成功写入：{trade_date_str}')

except Exception as e:
    print(f'拉取或插入失败：{trade_date_str}，错误：{e}')


# 关闭连接
cursor.close()
conn.close()
