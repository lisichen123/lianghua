import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr


def send_email(result):
    sender_email = "1114716195@qq.com"

    # ===================== 多邮箱配置（直接在这里加） =====================
    receiver_list = [
        "1114716195@qq.com",
        "172004133@qq.com",
        "304710380@qq.com"
    ]

    auth_code = "iwmlrydauaafgijf"
    smtp_server = "smtp.qq.com"
    smtp_port = 465

    # 列表转字符串，避免 encode 报错
    if isinstance(result, list):
        result_str = "\n".join([str(item) for item in result])
    else:
        result_str = str(result)

    # 构造邮件
    msg = MIMEText(result_str, "plain", "utf-8")
    msg['From'] = formataddr(("今日数据推送", sender_email))

    # ===================== 关键：多邮箱格式拼接 =====================
    msg['To'] = ", ".join(receiver_list)  # 必须用 逗号+空格 分隔
    msg['Subject'] = "✅ 今日股票数据已完成计算"

    try:
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(sender_email, auth_code)
            # sendmail 传入列表，直接群发
            server.sendmail(sender_email, receiver_list, msg.as_string())
        print("✅ 邮件发送成功（多个邮箱）！")
    except Exception as e:
        print(f"❌ 发送失败：{e}")
