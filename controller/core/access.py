# -*- coding: UTF-8 -*-
# Description:                    
# Author:           黄小雪
# Date:             2017年09月07日
# Company:          东方银谷
from public import *
from django.http import HttpResponse
from business_query.configuration.sqlList import *
from dtmt.query import DatabaseConnection
from functools import wraps
import json
import logging


def verification(check_class):
    """
    装饰器用于检测用户提交的信息是否合法.
    check_class 检测类
    Decorator for views that checks that the user submitted information,
    redirecting to the log-in page if necessary. The test should be a callable
    that takes the user object and returns True if the user passes.
    """

    def decorator(view_func):
        @wraps(view_func)
        def _wrapped_view(request, *args, **kwargs):
            response = HttpResponse()
            ccl = check_class(request)
            check_status, error_msg = ccl.total_check()
            if check_status:
                response.write(json.dumps({'status': check_status, 'msg': error_msg}))
                return response

            return view_func(request, *args, **kwargs)
        return _wrapped_view
    return decorator


class Check_IBQ(object):
    """
    检测投资批量查询提交的信息
    error_msg 存放所有错误消息
    check_status 错误状态 1 错误，0 正常，主要用于前端的JavaScript进行判断
    total_check 启动所有检测，返回检测状态和错误消息
    """
    def __init__(self, request):
        cur = Currency(request)
        rq_post = getattr(cur, 'rq_post')
        jdata = rq_post('data')
        data = json.loads(jdata)
        self.data = data
        self.conf = investment_batch_query_conf
        self.error_msg = []

    def check_data(self):
        # 检测脚本名称
        isdigit = [d for d in self.data if str(d).isdigit()]
        if not isdigit:
            self.error_msg.append(u'请输入手机号')
        else:
            if len(self.data) > self.conf['maxNum']:
                self.error_msg.append(u'每次查询量不能超过%s' % self.conf['maxNum'])

    def total_check(self):
        self.check_data()
        status = 1 if self.error_msg else 0

        return status, self.error_msg


class Check_PCI(object):
    """
    普惠离职员工客户信息查询
    error_msg 存放所有错误消息
    check_status 错误状态 1 错误，0 正常，主要用于前端的JavaScript进行判断
    total_check 启动所有检测，返回检测状态和错误消息
    """
    def __init__(self, request):
        cur = Currency(request)
        rq_post = getattr(cur, 'rq_post')
        jdata = rq_post('data')
        data = json.loads(jdata)
        self.logger = logging.getLogger('business_query')
        self.dc = DatabaseConnection(self.logger)
        self.data = data
        self.conf = puhuiCustomerInfoConf
        self.error_msg = []

    def check_data(self):
        # 检测脚本名称
        isdigit = [d for d in self.data if str(d)]
        if not isdigit:
            self.error_msg.append(u'请输入工号')
        else:
            if len(self.data) > self.conf['maxNum']:
                self.error_msg.append(u'每次查询量不能超过%s' % self.conf['maxNum'])

    def checkDateMaxNum(self):
        # 每天最多查询5次
        res = self.dc._r.hmget('puhuiCustomerInfo', 'count')[0]
        count = int(res) if res else 0
        if count >= 5:
            self.error_msg.append(u'今天已查询5次，欢迎明天再来查询！')

    def total_check(self):
        self.check_data()
        self.checkDateMaxNum()
        status = 1 if self.error_msg else 0

        return status, self.error_msg


class Check_people_upload(object):
    """
    检测人力上传的文件
    错误码 错误消息
    1     上传文件为空
    2     只能上传excel文件
    """
    def __init__(self, request):
        self.fileobj = request.FILES.get('file', None)
        self.error_msg = []
        self.error_code = []

    def check_file_exists(self):
        """
        检测文件是否存在
        :return:
        """
        if self.fileobj is None:
            self.error_msg.append(u'上传文件为空')
            self.error_code.append(1)

    def check_filename(self):
        """
        检测文件名后缀必须为xlsx、xls
        :return:
        """
        legal_file_suffixes = ['xlsx', 'xls']
        if self.fileobj.name.split('.')[-1] not in legal_file_suffixes:
            self.error_msg.append(u'只能上传excel文件')
            self.error_code.append(2)

    def total_check(self):
        self.check_file_exists()
        if 1 not in self.error_code:
            self.check_filename()
        status = 1 if self.error_msg else 0
        return status, self.error_msg