#! /usr/bin/env python
# -*- coding: UTF-8 -*-
##################################################
# Function:    银谷在线注册统计及出借统计脚本
# Usage:                python start.py
# Author:               黄小雪
# Date:				   2016年7月19日
# Company:
# Version:        1.2
##################################################

import os
import sys
import xlrd
import smtplib
import datetime
from email.header import Header
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.utils import parseaddr, formataddr

reload(sys)
sys.setdefaultencoding("utf-8")


class MailHelper(object):
    # 发html邮件
    def __init__(self, mail_host, mail_user, mail_pass,
                 sender, sender_zh_name, receivers, cc):

        # 第三方 SMTP 服务
        self.mail_host = mail_host  # 设置服务器
        self.mail_user = mail_user  # 用户名
        self.mail_pass = mail_pass  # 口令

        self.sender = sender
        self.sender_zh_name = sender_zh_name
        self.receivers = receivers  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
        self.cc = cc  # 抄送

        self.message = MIMEMultipart()  # 设置附件
        self.status = 0  # 执行状态
        self.msg = ''  # 错误消息

    def add_attch(self, res_file):
        # 添加附件
        # 附件为绝对路径 /opt/script/yingu_rt/res/充值提现明细.xls

        # 附件
        att1 = MIMEText(open(res_file, 'rb').read(), 'base64', 'utf-8')
        att1["Content-Type"] = 'application/octet-stream'
        # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
        att1["Content-Disposition"] = 'attachment; filename=%s' % Header(res_file.split('/')[-1], 'UTF-8')
        self.message.attach(att1)

    def insert_img(self, file):
        # 插入图片
        # 指定图片为当前目录
        fp = open(file, 'rb')
        msgImage = MIMEImage(fp.read())
        fp.close()

        # 定义图片 ID，在 HTML 文本中引用
        msgImage.add_header('Content-ID', '<image1>')
        self.message.attach(msgImage)

    def add_content(self, content, subject):
        self.message['Subject'] = Header(subject, 'utf-8')
        self.message['From'] = self._format_addr(u'%s <%s>'% (self.sender_zh_name, self.sender))
        self.message['To'] = ''.join(self._cvt_receivers(self.receivers))
        self.message['Cc'] = ''.join(self._cvt_receivers(self.cc))
        head_content = """"""
        mail_msg = ''.join([head_content, content])
        self.message.attach(MIMEText(mail_msg, 'html', 'utf-8'))

    def _cvt_receivers(self, receivers):
        # 收件人乱码处理
        return [self._cvt_user(u) for u in receivers]

    def _cvt_user(self, user):
        return ''.join(['<', user, '>'])

    def send_htm(self):
        # 发送邮件
        # 创建一个带附件的实例

        try:
            smtpObj = smtplib.SMTP()
            smtpObj.connect(self.mail_host, 25)  # 25 为 SMTP 端口号
            smtpObj.login(self.mail_user, self.mail_pass)
            smtpObj.sendmail(self.sender, self.receivers + self.cc, self.message.as_string())
        except smtplib.SMTPException, e:
            self.status = 1
            self.msg = u"%s" % e

    @staticmethod
    def _format_addr(s):
        name, addr = parseaddr(s)
        return formataddr((Header(name, 'utf-8').encode(),
                           addr.encode('utf-8') if isinstance(addr, unicode) else addr))
