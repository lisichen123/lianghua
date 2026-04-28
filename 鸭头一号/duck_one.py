import datetime
import re
import time

import pandas as pd

from send_mail_function import send_email
import pymysql
import numpy as np
import math
import json
import schedule
import time
import datetime
import mplfinance as mpf
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.platypus import Image
import os
from five_day_mv import select_up_ma5_stocks
# ===================== 数据库配置（和之前一致） =====================
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "123456",
    "db": "stock_data",
    "charset": "utf8mb4"
}
KLINE_DAYS = 30  # 近1个月K线（30个交易日）
PDF_SAVE_PATH = "今日股票K线图.pdf"  # PDF保存路径

mc = mpf.make_marketcolors(
    up='red',    # 上涨=红色
    down='green',# 下跌=绿色
    edge='inherit',
    wick='inherit',
    volume='inherit'
)
# 创建A股专用样式
a_stock_style = mpf.make_mpf_style(
    marketcolors=mc,
    gridstyle='',  # 无网格
    rc={'font.sans-serif': ['SimHei']}  # 中文不乱码
)

# 全局缓存交易日列表（避免每次查库）
trade_day_list = []

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


def cal_duck():
    # # =============================
    # # 鸭头判断核心参数（外部可调）鸭头一号
    # # =============================
    # # 1. 趋势强度
    # ANGLE_MIN = -0.3                  # 最小下滑角度（负数，越小越陡）
    # ANGLE_MAX = -0.01                   # 最大下滑角度（接近0表示平缓）
    # # 2. 线性拟合度 (关键：越接近 -1 越像直线下降)
    # CORR_THRESHOLD = -0.7
    # # 3. 波动控制
    # VOLATILITY_MAX = 0.02             # 残差标准差上限
    # # 4. 下跌幅度
    # DROP_MIN_RATIO = 0.009              # 整个窗口净下跌比例 (例如2%)
    # # 5. 反弹与单调性
    # MAX_REBOUND_RATIO = 0.08         # 允许从最低点反弹的最大比例
    # MONOTONIC_RATIO = 0.6           # 下跌步数占比（至少x%的天数在下跌）
    # WINDOW_MIN = 10
    # WINDOW_MAX = 15
    #
    # # =============================
    # # 鸭脖子（拉升）过滤参数
    # # =============================
    # SURGE_DAYS = 8           # 统计涨幅的天数（连续x天内）
    # SURGE_RATE = 0.30         # 涨幅阈值（x0% = 0.x0）

    # =============================
    # 鸭头判断核心参数（外部可调）
    # =============================
    # 1. 趋势强度
    ANGLE_MIN = -0.3  # 最小下滑角度（负数，越小越陡）
    ANGLE_MAX = -0.01  # 最大下滑角度（接近0表示平缓）
    # 2. 线性拟合度 (关键：越接近 -1 越像直线下降)
    CORR_THRESHOLD = -0.7
    # 3. 波动控制
    VOLATILITY_MAX = 0.02  # 残差标准差上限
    # 4. 下跌幅度
    DROP_MIN_RATIO = 0.009  # 整个窗口净下跌比例 (例如2%)
    # 5. 反弹与单调性
    MAX_REBOUND_RATIO = 0.08  # 允许从最低点反弹的最大比例
    MONOTONIC_RATIO = 0.6  # 下跌步数占比（至少x%的天数在下跌）
    WINDOW_MIN = 10
    WINDOW_MAX = 15

    # =============================
    # 鸭脖子（拉升）过滤参数
    # =============================
    SURGE_DAYS = 8  # 统计涨幅的天数（连续x天内）
    SURGE_RATE = 0.30  # 涨幅阈值（x0% = 0.x0）

    # =============================
    # 数据库连接
    # =============================

    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="123456",
        database="stock_data",
        charset="utf8mb4"
    )

    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT ts_code FROM stock_forward_adjusted")
    stocks = [row[0] for row in cursor.fetchall()]

    print("股票数量:", len(stocks))

    # =============================
    # 函数：二次验证是否存在显著拉升
    # =============================
    def has_neck_surge(ts_code, cursor):
        """
        针对已经入选的股票，回溯其历史价格，看是否存在短期暴力拉升
        """
        # 重新查询该股的所有历史收盘价（按时间正序）
        sql = """
            SELECT close 
            FROM (
                -- 子查询先按日期排序并添加行号
                SELECT 
                    close,
                    trade_date,
                    -- 按日期降序排列，最新的日期行号为1
                    ROW_NUMBER() OVER (ORDER BY trade_date DESC) AS rn
                FROM stock_forward_adjusted 
                WHERE ts_code = %s
            ) t
            -- 筛选出行号 <= 22 的记录（最新的22个交易日）
            WHERE rn <= 30
            -- 最终结果按日期升序排列
            ORDER BY trade_date ASC
        """
        cursor.execute(sql, (ts_code,))
        rows = cursor.fetchall()

        if len(rows) < SURGE_DAYS + 1:
            return False

        # 提取价格列表
        prices = [float(row[0]) for row in rows]

        # 滑动窗口检测
        # 遍历价格，计算当前价格相对于前 N 天的价格涨幅
        for i in range(SURGE_DAYS, len(prices)):
            past_price = prices[i - SURGE_DAYS]
            current_price = prices[i]

            if past_price <= 0: continue

            # 计算涨幅
            increase = (current_price - past_price) / past_price

            if increase >= SURGE_RATE:
                # 只要找到一段符合要求的拉升，就返回 True
                return True

        return False

    # =============================
    # 计算 MA5
    # =============================

    def calc_ma5(close_list):

        ma5 = []

        for i in range(len(close_list)):
            if i < 4:
                ma5.append(None)
            else:
                ma5.append(sum(close_list[i - 4:i + 1]) / 5)

        return ma5

    # =============================
    # 鸭头判断（核心修改）
    # =============================
    def is_duck_head(ma5_slice):
        y = np.array(ma5_slice)
        n = len(y)

        # 1. 归一化 (以初始价格为基准)
        y_norm = y / y[0]
        x = np.arange(n)

        # 2. 线性拟合与角度
        k, b = np.polyfit(x, y_norm, 1)
        degree = math.atan(k) * 180 / math.pi

        # 3. 计算皮尔逊相关系数 (衡量下降的连续性)
        # r 越接近 -1，代表下行趋势越稳，波动越小
        r = np.corrcoef(x, y_norm)[0, 1]

        # 4. 波动率（残差标准差）
        y_pred = k * x + b
        volatility = np.std(y_norm - y_pred)

        # 5. 净跌幅与反弹
        net_drop = (y[0] - y[-1]) / y[0]
        current_rebound = (y[-1] - np.min(y)) / y[0]

        # 6. 单调性得分 (下跌的天数占比)
        diffs = np.diff(y)
        down_days_ratio = np.sum(diffs < 0) / len(diffs)

        # ---------- 判断逻辑 ----------

        # 条件A: 角度在范围内
        cond_angle = ANGLE_MIN < degree < ANGLE_MAX
        # 条件B: 相关系数足够负 (确保是一直在降)
        cond_corr = r < CORR_THRESHOLD
        # 条件C: 波动不能太大
        cond_vol = volatility < VOLATILITY_MAX
        # 条件D: 跌幅达标且反弹微弱
        cond_drop = net_drop > DROP_MIN_RATIO and current_rebound < MAX_REBOUND_RATIO
        # 条件E: 大多数时候都在降
        cond_mono = down_days_ratio > MONOTONIC_RATIO

        if all([cond_angle, cond_corr, cond_vol, cond_drop, cond_mono]):
            ######### 调试日志（可选）
            # print(f"Match! Degree:{degree:.2f}, R:{r:.2f}, Vol:{volatility:.4f}, Mono:{down_days_ratio:.2f}")
            # print(y_norm)
            return True

        return False

    # =============================
    # 只检测今天的窗口
    # =============================

    def detect_duck(ma5):

        n = len(ma5)

        for window in range(WINDOW_MIN, WINDOW_MAX + 1):

            start = n - window
            end = n

            if start < 0:
                continue

            ma5_slice = ma5[start:end]

            if None in ma5_slice:
                continue

            if is_duck_head(ma5_slice):
                return True

        return False

    # =============================
    # 主扫描
    # =============================

    duck_list = []

    for ts_code in stocks:
        sql = """
        SELECT trade_date, close
        FROM stock_forward_adjusted
        WHERE ts_code=%s
        ORDER BY trade_date
        """

        cursor.execute(sql, (ts_code,))
        rows = cursor.fetchall()

        if len(rows) < 35:
            continue

        close_list = [float(r[1]) for r in rows]
        ma5 = calc_ma5(close_list)
        if detect_duck(ma5):
            last_date = rows[-1][0]

            duck_list.append((ts_code, last_date))

            # print("发现鸭头:", ts_code, last_date)

    print("--------------")
    print("鸭头股票数量:", len(duck_list))

    print("--- 开始二次筛选：检测前期拉升 ---")
    final_selected = []

    for s in duck_list:
        ts_code = s[0]
        last_date = s[1]

        # 1. 过滤北交所
        if 'BJ' in ts_code:
            continue

        # 2. 调用函数判断是否有“鸭脖子”拉升
        if has_neck_surge(ts_code, cursor):
            final_selected.append((ts_code, last_date))
            print(f"✅ 符合老鸭头形态: {ts_code} (包含显著拉升)")
        else:
            # 如果需要调试，可以打印没通过的
            # print(f"❌ 剔除 (无显著拉升): {ts_code}")
            pass

    cursor.close()
    conn.close()
    return final_selected

def format_duck_stock(duck_data):
    # 如果没有数据
    if not duck_data:
        return "暂无符合条件股票"
    # 遍历格式化每一只股票
    stock_lines = []
    for ts_code, trade_date in duck_data:
        # 把日期转成 2026-04-23 格式
        date_str = trade_date.strftime("%Y-%m-%d")
        stock_lines.append(f"股票：{ts_code} | 日期：{date_str}")
    # 换行拼接
    return "\n".join(stock_lines)


def get_stock_kline(ts_code):
    """
    拉取单只股票近30个交易日K线数据
    返回：pandas.DataFrame（标准K线格式）
    """
    conn = pymysql.connect(**MYSQL_CONFIG)
    # 查询近30个交易日数据，按日期升序
    sql = f"""
        SELECT trade_date, open, high, low, close, vol 
        FROM stock_forward_adjusted 
        WHERE ts_code = %s 
        ORDER BY trade_date DESC 
        LIMIT {KLINE_DAYS}
    """
    df = pd.read_sql(sql, conn, params=(ts_code,))
    conn.close()

    # 数据格式化（mplfinance专用）
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values('trade_date').set_index('trade_date')
    df.rename(columns={'vol': 'volume'}, inplace=True)
    return df


# ===================== 3. 绘制单只股票K线图（带MA5） =====================
def plot_kline(df, ts_code):
    """
    绘制K线图，返回图片临时路径
    """
    img_path = f"temp_{ts_code}.png"
    # 绘图配置：K线 + MA5 + 成交量
    mpf.plot(
        df,
        type='candle',  # 蜡烛图
        mav=5,  # 5日均线
        volume=True,  # 成交量
        title=f'{ts_code} 近1个月K线图',
        style=a_stock_style,  # 正确样式
        savefig=img_path
    )
    return img_path


# ===================== 4. 批量画图 + 生成PDF =====================
def create_kline_pdf(stock_codes):
    """
    批量画K线图，合并生成PDF
    """
    if not stock_codes:
        print("无股票，跳过生成PDF")
        return None

    # 创建PDF
    c = canvas.Canvas(PDF_SAVE_PATH, pagesize=A4)
    width, height = A4

    print(f"\n开始绘制 {len(stock_codes)} 只股票K线图...")
    temp_imgs = []

    for code in stock_codes:
        try:
            # 拉数据 + 画图
            df = get_stock_kline(code)
            if df.empty:
                print(f"{code} 无数据，跳过")
                continue

            img_path = plot_kline(df, code)
            temp_imgs.append(img_path)

            # 插入PDF（一页一只股票）
            img = Image(img_path)
            img.drawHeight = height * 0.85
            img.drawWidth = width * 0.95
            img.drawOn(c, width * 0.025, height * 0.05)
            c.showPage()  # 新建一页
            print(f"✅ 绘制完成：{code}")
        except Exception as e:
            print(f"❌ 绘制失败：{code}，错误：{e}")

    # 保存PDF + 删除临时图片
    c.save()
    for img in temp_imgs:
        if os.path.exists(img):
            os.remove(img)

    print(f"\nPDF生成完成！路径：{os.path.abspath(PDF_SAVE_PATH)}")
    return PDF_SAVE_PATH

def extract_all_stock_codes(duck_data, five_day_msg):
    """
    严格顺序：先鸭头 → 后五日均线，不乱序、不打乱
    """
    codes = []
    seen = set()

    # ① 优先：鸭头形态 原始顺序
    for item in duck_data:
        c = item[0]
        if c not in seen:
            seen.add(c)
            codes.append(c)

    # ② 其次：五日均线 从上到下顺序
    pattern = r'([0-9]{6}\.[SHSZ]{2})'
    five_codes = re.findall(pattern, five_day_msg)
    for c in five_codes:
        if c not in seen:
            seen.add(c)
            codes.append(c)

    # 完全按：鸭头在先、五日在后，顺序不变
    return codes

# 2. 核心任务：每晚23:30执行，判断当天是否是交易日
def check_today_is_trade_day():
    # 获取今天日期 格式：2026-01-05
    today = datetime.date.today().strftime("%Y-%m-%d")
    print(f"\n🕘 定时任务执行：当前检测日期 = {today}")
    if today in trade_day_list:
        print(f"✅ {today} 是股票交易日/工作日")
        # ------------------- 你的原有主逻辑（仅修改拼接部分） -------------------
        today_duck_stock = cal_duck()
        print('鸭头计算完成')
        time.sleep(1)

        five_day_mv = select_up_ma5_stocks()
        print('五日均线选股完成')
        time.sleep(1)

        # 格式化鸭头数据
        formatted_duck = format_duck_stock(today_duck_stock)

        # 最终拼接（整洁换行，邮件完美显示）
        final_data = f"""【鸭头形态选股结果】
        {formatted_duck}
        
        【沿5日均线强势上升选股结果】
        {five_day_mv}"""

        # ===================== 【新增：自动绘制K线图 + 生成PDF】 =====================
        # 1. 提取所有股票代码
        all_stocks = extract_all_stock_codes(today_duck_stock, five_day_mv)
        # 2. 生成PDF
        pdf_file = create_kline_pdf(all_stocks, )

        # 发送邮件
        send_email(final_data, pdf_file)  # 注意：send_email需要支持附件，我下面给你改好！

        # 这里可以加你业务：发通知、跑脚本、推送消息等
    else:
        print(f"❌ {today} 非股票交易日/节假日/周末")

# 3. 初始化：先加载一次日期列表
load_trade_days()

# 4. 设置定时：每天晚上23点30分执行
schedule.every().day.at("23:30").do(check_today_is_trade_day)

# 5. 死循环跑定时
if __name__ == '__main__':
    print("⏰ 定时服务已启动，每日23:30自动检测当日是否为交易日...")
    while True:
        schedule.run_pending()
        time.sleep(28)  # 30秒轮询一次，轻量化不耗性能
