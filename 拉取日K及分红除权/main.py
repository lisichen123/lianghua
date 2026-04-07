from pull_corp_today_data import pull_today_data
from cal_all_corp_forward_price_daily import cal_forward_price_daily
import json
import schedule
import time
import datetime
import pymysql

# ===================== 数据库配置（和之前一致） =====================
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "123456",
    "db": "stock_data",
    "charset": "utf8mb4"
}

# 全局缓存交易日列表（避免每次查库）
trade_day_list = []


# 1. 从MySQL读取交易日列表
def load_trade_days():
    global trade_day_list
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT stock_day_list FROM stock_day_table LIMIT 1")
        res = cursor.fetchone()
        if res:
            trade_day_list = json.loads(res[0])
            print(f"✅ 已加载交易日总数：{len(trade_day_list)}")
        conn.close()
    except Exception as e:
        print(f"❌ 读取数据库失败：{e}")


# 2. 核心任务：每晚22:30执行，判断当天是否是交易日
def check_today_is_trade_day():
    # 获取今天日期 格式：2026-01-05
    today = datetime.date.today().strftime("%Y-%m-%d")
    print(f"\n🕘 定时任务执行：当前检测日期 = {today}")

    if today in trade_day_list:
        print(f"✅ {today} 是股票交易日/工作日")
        pull_today_data()
        print('拉取今日数据完成')
        time.sleep(10)
        cal_forward_price_daily()
        print('分红除权完成')

        # 这里可以加你业务：发通知、跑脚本、推送消息等
    else:
        print(f"❌ {today} 非股票交易日/节假日/周末")


# 3. 初始化：先加载一次日期列表
load_trade_days()

# 4. 设置定时：每天晚上22点30分执行
schedule.every().day.at("22:30").do(check_today_is_trade_day)

# 5. 死循环跑定时
if __name__ == '__main__':
    print("⏰ 定时服务已启动，每日22:30自动检测当日是否为交易日...")
    while True:
        schedule.run_pending()
        time.sleep(30)  # 30秒轮询一次，轻量化不耗性能

