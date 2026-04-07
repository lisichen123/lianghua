import pymysql
from datetime import datetime, timedelta

def cal_forward_price_daily():
    # 配置 MySQL 连接
    conn1 = pymysql.connect(
        host='localhost',
        user='root',
        password='123456',
        database='stock_data',
        charset='utf8mb4'
    )
    cursor1 = conn1.cursor()

    ####删除前复权表中所有数据
    sql = "TRUNCATE TABLE stock_forward_adjusted"
    cursor1.execute(sql)
    conn1.commit()

    today = datetime.today().date()
    days_60_ago = today - timedelta(days=90)

    # 从不复权表中取数据
    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="123456",
        database="stock_data",
        charset="utf8mb4"
    )

    cursor = conn.cursor(pymysql.cursors.DictCursor)

    sql = """
    SELECT *
    FROM stock_backward_adjusted
    WHERE trade_date >= %s
    ORDER BY ts_code, trade_date DESC
    """

    cursor.execute(sql, (days_60_ago,))
    rows = cursor.fetchall()

    # 按股票分组
    stock_data = {}
    for r in rows:
        stock_data.setdefault(r["ts_code"], []).append(r)

    insert_list = []

    for ts_code, data in stock_data.items():

        curr_close = round(float(data[0]["close"]), 2)

        for i, row in enumerate(data):

            trade_date = row["trade_date"]

            open_p = round(float(row["open"]), 2)
            high_p = round(float(row["high"]), 2)
            low_p = round(float(row["low"]), 2)
            close_p = round(float(row["close"]), 2)

            # 最新一天不动
            if i == 0:

                new_open = open_p
                new_high = high_p
                new_low = low_p
                new_close = close_p

            else:

                # ⭐关键修正：使用下一天的涨跌幅
                pct = round(float(data[i - 1]["pct_chg"]), 2)

                prev_close = round(curr_close / (1 + pct / 100), 2)

                ratio_open = open_p / close_p if close_p else 1
                ratio_high = high_p / close_p if close_p else 1
                ratio_low = low_p / close_p if close_p else 1

                new_open = round(prev_close * ratio_open, 2)
                new_high = round(prev_close * ratio_high, 2)
                new_low = round(prev_close * ratio_low, 2)
                new_close = prev_close

                curr_close = prev_close

            # 复制整行数据
            new_row = list(row.values())

            col_index = list(row.keys())

            new_row[col_index.index("open")] = round(new_open, 2)
            new_row[col_index.index("high")] = round(new_high, 2)
            new_row[col_index.index("low")] = round(new_low, 2)
            new_row[col_index.index("close")] = round(new_close, 2)

            insert_list.append(tuple(new_row))

    # 构造 INSERT
    columns = list(rows[0].keys())
    col_sql = ",".join(columns)
    placeholder = ",".join(["%s"] * len(columns))

    insert_sql = f"""
    INSERT INTO stock_forward_adjusted ({col_sql})
    VALUES ({placeholder})
    """

    cursor.executemany(insert_sql, insert_list)

    conn.commit()

    cursor.close()
    conn.close()