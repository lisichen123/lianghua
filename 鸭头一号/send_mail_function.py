import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr

def send_email(result):
    sender_email = "1114716195@qq.com"
    receiver_email = "1114716195@qq.com"
    auth_code = "iwmlrydauaafgijf"

    smtp_server = "smtp.qq.com"
    smtp_port = 465

    # ✅ 修复：列表自动转成字符串，解决 encode 报错
    if isinstance(result, list):
        # 分行显示，邮件里看得清清楚楚
        result_str = "\n".join([str(item) for item in result])
    else:
        result_str = str(result)

    # 邮件内容
    msg = MIMEText(result_str, "plain", "utf-8")
    msg['From'] = formataddr(("今日数据推送", sender_email))
    msg['To'] = formataddr(("自己", receiver_email))
    msg['Subject'] = "✅ 今日股票数据已完成计算"

    try:
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(sender_email, auth_code)
            server.sendmail(sender_email, receiver_email, msg.as_string())
        print("✅ 邮件发送成功！")
    except Exception as e:
        print(f"❌ 发送失败：{e}")