import datetime
import time

import pymysql
import numpy as np
import math

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
ANGLE_MIN = -0.3                  # 最小下滑角度（负数，越小越陡）
ANGLE_MAX = -0.01                   # 最大下滑角度（接近0表示平缓）
# 2. 线性拟合度 (关键：越接近 -1 越像直线下降)
CORR_THRESHOLD = -0.7
# 3. 波动控制
VOLATILITY_MAX = 0.02             # 残差标准差上限
# 4. 下跌幅度
DROP_MIN_RATIO = 0.009              # 整个窗口净下跌比例 (例如2%)
# 5. 反弹与单调性
MAX_REBOUND_RATIO = 0.08         # 允许从最低点反弹的最大比例
MONOTONIC_RATIO = 0.6           # 下跌步数占比（至少x%的天数在下跌）
WINDOW_MIN = 10
WINDOW_MAX = 15

# =============================
# 鸭脖子（拉升）过滤参数
# =============================
SURGE_DAYS = 8           # 统计涨幅的天数（连续x天内）
SURGE_RATE = 0.30         # 涨幅阈值（x0% = 0.x0）


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
            ma5.append(sum(close_list[i-4:i+1]) / 5)

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