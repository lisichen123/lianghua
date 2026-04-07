import json
import datetime
import pymysql
from pymysql.err import ProgrammingError

# ====================== 1. 2026 节假日列表（你提供的）======================
holiday_2026 = [
    "2026-01-01", "2026-01-02", "2026-01-03",
    "2026-02-15", "2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19",
    "2026-02-20", "2026-02-21", "2026-02-22", "2026-02-23",
    "2026-04-04", "2026-04-05", "2026-04-06",
    "2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04", "2026-05-05",
    "2026-06-19", "2026-06-20", "2026-06-21",
    "2026-09-25", "2026-09-26", "2026-09-27",
    "2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04",
    "2026-10-05", "2026-10-06", "2026-10-07"
]

# ====================== 2. 生成 2026 全年工作日（扣除节假日 + 周末）======================
def get_work_days():
    start = datetime.date(2026, 1, 1)
    end = datetime.date(2026, 12, 31)
    delta = datetime.timedelta(days=1)

    work_days = []
    current = start

    while current <= end:
        date_str = current.strftime("%Y-%m-%d")

        # 排除：周末（周六=5，周日=6）
        if current.weekday() >= 5:
            current += delta
            continue

        # 排除：法定节假日
        if date_str in holiday_2026:
            current += delta
            continue

        # 剩下的就是工作日
        work_days.append(date_str)
        current += delta

    return work_days

# 获取最终工作日列表
work_day_list = get_work_days()
print("2026年工作日总数：", len(work_day_list))
print("工作日示例（前10条）：", work_day_list[:10])

# 转为 JSON 字符串
work_day_json = json.dumps(work_day_list, ensure_ascii=False)

# ====================== 3. MySQL 配置（请你修改这里）======================
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",          # 你的 MySQL 用户名
    "password": "123456",    # 你的 MySQL 密码
    "charset": "utf8mb4"
}

# ====================== 4. 连接 MySQL 并创建库、表、插入数据 ======================
def save_to_mysql(json_data):
    conn = None
    try:
        # 1. 连接 MySQL
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        # 2. 创建数据库 stock_data（不存在则创建）
        cursor.execute("CREATE DATABASE IF NOT EXISTS stock_data DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        print("✅ 数据库 stock_data 已确保存在")

        # 3. 使用该库
        cursor.execute("USE stock_data")

        # 4. 创建表 stock_day_table（只有一列：stock_day_list）
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS stock_day_table (
            id INT PRIMARY KEY AUTO_INCREMENT,
            stock_day_list JSON NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        cursor.execute(create_table_sql)
        print("✅ 表 stock_day_table 已确保存在")

        # 5. 清空旧数据（避免重复）
        cursor.execute("TRUNCATE TABLE stock_day_table")

        # 6. 插入 JSON 数据
        insert_sql = "INSERT INTO stock_day_table (stock_day_list) VALUES (%s)"
        cursor.execute(insert_sql, json_data)
        conn.commit()

        print("✅ 2026 工作日 JSON 已成功写入 MySQL！")

    except ProgrammingError as e:
        print("❌ MySQL 错误：", e)
    finally:
        if conn:
            conn.close()

# 执行保存
save_to_mysql(work_day_json)