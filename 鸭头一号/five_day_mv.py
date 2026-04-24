import pymysql
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

# ===================== 1. 基础配置 =====================
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'stock_data',
    'charset': 'utf8mb4'
}
MIN_DAYS = 5    # 最小连续天数
MAX_DAYS = 15   # 最近15天窗口
FAULT_RATE = 0.001  # 0.1%容错

# ===================== 2. 数据库工具函数 =====================
def get_db_connection():
    return pymysql.connect(**DB_CONFIG)

def get_recent_trade_days(days=MAX_DAYS):
    """获取最近15个真实交易日，升序排列（旧→新）"""
    conn = get_db_connection()
    sql = f"""
        SELECT DISTINCT trade_date 
        FROM stock_backward_adjusted 
        ORDER BY trade_date DESC 
        LIMIT {days}
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    return sorted(df['trade_date'].astype(str).tolist())

def batch_fetch_stock_data(trade_days):
    """一次性拉取所有股票15天数据，严格按【股票+日期升序】排序"""
    conn = get_db_connection()
    day_str = "','".join(trade_days)
    sql = f"""
        SELECT ts_code, trade_date, close, low 
        FROM stock_backward_adjusted 
        WHERE trade_date IN ('{day_str}')
        ORDER BY ts_code, trade_date ASC  # 关键：日期旧→新，保证MA5计算正确
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    return df

# ===================== 3. 核心修复：MA5计算 + 连续天数 =====================
def calc_ma5_and_filter(df):
    # 1. 【正确MA5计算】：日期升序，groupby后滚动5天均值（完全对齐你的算法）
    df['ma5'] = df.groupby('ts_code')['close'].rolling(window=5).mean().reset_index(0, drop=True)
    df = df.dropna(subset=['ma5'])  # 剔除前4天无MA5的数据

    # 2. 核心条件：最低价 不破MA5（0.1%容错）
    df['keep_ma5'] = df['low'] >= df['ma5'] * (1 - FAULT_RATE)

    # 3. 趋势条件：MA5连续向上（当日MA5 ≥ 前一日）
    df['ma5_rise'] = df.groupby('ts_code')['ma5'].diff() >= 0

    # 4. 合并双条件：同时满足才算有效
    df['valid'] = df['keep_ma5'] & df['ma5_rise']

    # 5. 【关键修复】计算【最近连续满足天数】，断一天就清零
    def calc_recent_consecutive(group):
        count = 0
        # 按日期旧→新遍历，计算连续天数
        for v in group['valid']:
            count = count + 1 if v else 0
        return count  # 只返回【最终连续天数】

    # 6. 批量计算每只股票的【最近连续天数】
    result = df.groupby('ts_code').apply(calc_recent_consecutive).reset_index()
    result.columns = ['ts_code', 'recent_consec_days']

    # 7. 最终筛选：最近连续≥5天即符合
    target = result[result['recent_consec_days'] >= MIN_DAYS].copy()
    return target

# ===================== 4. 主函数（关键修改：返回可发送的文本变量） =====================
def select_up_ma5_stocks():
    print("="*60)
    print("【修复版】筛选：最近连续5-15天 沿MA5向上、不破均线")
    print("MA5规则：当日+前4天收盘价平均 | 连续规则：断一天即清零")
    print("="*60)

    trade_days = get_recent_trade_days()
    print(f"最近交易日：{trade_days}")
    
    df = batch_fetch_stock_data(trade_days)
    print(f"总股票数：{df['ts_code'].nunique()}只")
    
    target_stocks = calc_ma5_and_filter(df)
    
    # ===================== 核心修改：集成结果为发送变量，无循环打印 =====================
    send_msg = ""  # 定义发送用的变量
    if target_stocks.empty:
        send_msg = "✅ 选股结果：暂无符合条件股票"
    else:
        # 排序 + 拼接成完整文本（直接集成在变量里，一行一只股票）
        target_stocks = target_stocks.sort_values('recent_consec_days', ascending=False)
        send_msg = "✅ 选股结果（沿5日线强势上升）：\n"
        # 批量拼接所有股票，不循环打印
        stock_list = [f"股票：{row['ts_code']} | 连续满足：{row['recent_consec_days']}天" 
                      for _, row in target_stocks.iterrows()]
        send_msg += "\n".join(stock_list)
    
    # 打印最终结果（仅一次）
    print("-"*60)
    print(send_msg)
    
    # 返回这个变量，你后续直接发送即可！
    return send_msg

# ===================== 运行入口 =====================
if __name__ == '__main__':
    # 直接获取可发送的文本结果
    result_msg = select_up_ma5_stocks()
    
    # 你后续的发送代码，直接用 result_msg 就行！
    # 例：send_to_wechat(result_msg)
    # 例：send_to_dingding(result_msg)
