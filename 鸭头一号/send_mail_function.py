import smtplib
import os  # 新增：判断文件是否存在
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart  # 新增：支持附件
from email.mime.application import MIMEApplication  # 新增：PDF附件
from email.utils import formataddr
from datetime import datetime  # 新增：自动加今日日期

def send_email(result, pdf_path=None):  # 新增参数：pdf_path（附件路径）
    sender_email = "1114716195@qq.com"

    # 多邮箱配置（完全保留）
    receiver_list = [
        "1114716195@qq.com",
        "172004133@qq.com",
        "304710380@qq.com"
    ]

    auth_code = "iwmlrydauaafgijf"
    smtp_server = "smtp.qq.com"
    smtp_port = 465

    # 处理正文文本（保留原有逻辑）
    if isinstance(result, list):
        result_str = "\n".join([str(item) for item in result])
    else:
        result_str = str(result)

    # ===================== 核心修改：支持附件的邮件结构 =====================
    # 改为多部件邮件（正文+附件）
    msg = MIMEMultipart()
    msg['From'] = formataddr(("今日数据推送", sender_email))
    msg['To'] = ", ".join(receiver_list)
    # 自动添加今日日期（你之前要求的）
    today = datetime.now().strftime("%Y-%m-%d")
    msg['Subject'] = f"✅ {today} 股票数据已完成计算（附件为K线图）"

    # 添加邮件正文
    msg.attach(MIMEText(result_str, "plain", "utf-8"))

    # ===================== 新增：自动添加PDF附件 =====================
    if pdf_path is not None and os.path.exists(pdf_path):
        try:
            with open(pdf_path, "rb") as f:
                # 读取PDF文件
                pdf_attach = MIMEApplication(f.read(), _subtype="pdf")
                # 设置附件名称
                pdf_attach.add_header("Content-Disposition", "attachment", filename=os.path.basename(pdf_path))
                msg.attach(pdf_attach)
            print(f"📎 已添加附件：{os.path.basename(pdf_path)}")
        except Exception as e:
            print(f"⚠️ 附件添加失败：{e}")

    # 发送逻辑（完全保留）
    try:
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(sender_email, auth_code)
            server.sendmail(sender_email, receiver_list, msg.as_string())
        print("✅ 邮件发送成功（含附件/群发）！")
        # ===================== 发送成功后删除PDF文件 =====================
        if pdf_path is not None and os.path.exists(pdf_path):
            os.remove(pdf_path)
            print(f"🗑️ 已自动删除附件文件：{os.path.basename(pdf_path)}")

    except Exception as e:
        print(f"❌ 发送失败：{e}")