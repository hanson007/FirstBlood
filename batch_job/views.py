# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib.auth.decorators import permission_required
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.shortcuts import render
from django.http import HttpResponse
from functools import wraps
from controller.core.public import (Currency, DatetimeHelp)
from controller.core import query_sql
from controller.public import dataconn
from djcelery import loaders
from djcelery.models import PeriodicTask, CrontabSchedule
from djcelery.schedulers import ModelEntry
# from multiprocessing import Manager,Pool
import multiprocessing as mp
from anyjson import loads, dumps
from celery import shared_task
from celery import registry
from celery import schedules
from conf import config
from datax_web.conf import config as datax_web_config
from datax_web.views import run as datax_web_run
from datax_web.views import Datax
from datax_web.views import JobData as DataxJobData
import commands
import logging
import sys
import json
reload(sys)
sys.setdefaultencoding("utf-8")
# Create your views here.


logger = logging.getLogger('batch_job')
_SUCCESS = dict(status=0, msg=u'检测成功')
_str = (str, unicode)
# 操作类型
_OPERATION_TYPE = (1, 2)  # 1:新增批处理作业  2:修改批处理作业
# 触发模式
_TRIGGER_MODE = (1, 2)  # 1:自动  2:手动
_TRIGGER_MODE_STR = u'TRIGGER_MODE = (1, 2)  # 1:自动  2:手动'
# 子作业类型
_SUBJOB_TYPE = (1, 2, 3)  # 1 数据同步 2 SQL脚本 3 备份。 主要用于后期扩展
# 执行状态
_STATUS = (0, 1)  # 状态 0 正在执行 1 执行完成


def verification(CheckClass):
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
            ccl = CheckClass(request)
            result = ccl.total_check()
            if result['status']:
                response.write(json.dumps(result))
                return response

            return view_func(request, *args, **kwargs)
        return _wrapped_view
    return decorator


class BatchJobData(object):
    """
    新增、更新、手工运行任务时处理数据
    """
    def __init__(self, data):
        # id 为 batch_job_id
        self._batch_job_id = data.get('_id', None)
        self.name = data.get('name', '')
        self.description = data.get('description', '')
        self.task_template = data.get('task_template', '')
        self.is_enable = data.get('is_enable', '')
        self.crontab = data.get('crontab', '')
        self.batch_job_details = data.get('batch_job_details', [])
        self.trigger_mode = data.get('trigger_mode', '')
        self.operation_type = data.get('operation_type', '')

        self.dtconn = dataconn.DatabaseConnection(logger)
        self.dtsf = dataconn.DataTransform()
        self.dh = DatetimeHelp()
        self.__timestamp1 = self.dh.timestamp1


    @property
    def timestamp1(self):
        return self.__timestamp1

    def _get_schedule_dict(self):
        schedule = CrontabSchedule.objects.get(pk=self.crontab)
        return {
            'crontab': schedule,
            'kwargs': dumps({}),
            'task': self.task_template,
            'enabled': self.is_enable,
            'name': self.name
        }

    @property
    def schedule_dict(self):
        return self._get_schedule_dict()

    # def get_batch_job_id(self):
    #     """
    #     新建批处理任务数据时 batch_id 为 插入数据后的返回id
    #     更新或手动运行任务时 batch_id 为 页面体检的id
    #     :return: batch_id
    #     """
    #     return self._batch_job_id or self.dtconn.ygol.insert_id

    @property
    def batch_job_id(self):
        """
        新建批处理任务数据时 batch_id 为 插入数据后的返回id
        更新或手动运行任务时 batch_id 为 页面体检的id
        :return: batch_id
        """
        return self._batch_job_id

    @batch_job_id.setter
    def batch_job_id(self, batch_job_id):
        self._batch_job_id = batch_job_id

    def get_insert_datax_job_sql(self):
        # 在 batch_job表里创建新的任务  -  新增SQL
        return config.insert_batch_job_sql % (self.name, self.description)

    def get_update_batch_job_by_id_sql(self):
        # 获取batch_job表里更新任务  - 更新SQL
        return config.update_batch_job_by_id_sql % (self.name, self.description, self.batch_job_id)

    def get_insert_batch_job_details_sql(self):
        # 拼接插入批处理作业详情表SQL  insert into  values ('user_id', 1), ('card_name', 1)
        values_list = ["('%s', %s, %s)" % (self.batch_job_id, subjob['subjob_id'], subjob['type']) for subjob in self.batch_job_details]
        return config.insert_batch_job_details_sql + ','.join(values_list)

    def get_delete_batch_job_details_by_id_sql(self):
        # 获取根据ID删除batch_job_details 表的sql
        return config.delete_batch_job_details_by_id_sql % self.batch_job_id

    def create(self):
        # batch_job表里创建新的任务
        result = _SUCCESS.copy()
        sql1 = self.get_insert_datax_job_sql()
        self.dtconn.ygol.transaction_start()
        self.dtconn.ygol.transaction_execute(sql1)
        if self.dtconn.ygol.status:
            msg = u'batch_job表里创建新的任务，SQL：%s 插入数据失败。 -  Msg: %s' % \
                  (sql1, self.dtconn.ygol.msg)
            logger.error(msg)
            result = dict(status=500, msg=msg)
        else:
            self.batch_job_id = self.dtconn.ygol.insert_id
            sql2 = self.get_insert_batch_job_details_sql()
            self.dtconn.ygol.transaction_execute(sql2)
            if self.dtconn.ygol.status:
                msg = u'创建批处理作业详情，SQL：%s 插入数据失败。 -  Msg: %s' % \
                      (sql2, self.dtconn.ygol.msg)
                logger.error(msg)
                result = dict(status=500, msg=msg)
        self.dtconn.ygol.transaction_commit_and_close()
        return result

    def create_PeriodicTask(self):
        """
        创建定时任务

            在 PeriodicTask表里创建新的定时任务，并在表的args字段里保存批处理作业表的batch_job_id。这样，
            就可以把批处理作业和定时调度关联起来。
        :return:
        """
        obj = PeriodicTask.objects.create(**self.schedule_dict)
        obj.args = dumps([self.batch_job_id])
        obj.save()
        return obj

    def update(self):
        # 更新任务
        result = _SUCCESS.copy()
        sql1 = self.get_update_batch_job_by_id_sql()
        self.dtconn.ygol.transaction_start()
        self.dtconn.ygol.transaction_execute(sql1)
        if self.dtconn.ygol.status:
            msg = u'batch_job表，SQL：%s 更新数据失败。 -  Msg: %s' % \
                  (sql1, self.dtconn.ygol.msg)
            logger.error(msg)
            result = dict(status=500, msg=msg)
        else:
            sql2 = self.get_delete_batch_job_details_by_id_sql()
            sql3 = self.get_insert_batch_job_details_sql()
            self.dtconn.ygol.transaction_execute(sql2)
            self.dtconn.ygol.transaction_execute(sql3)
            if self.dtconn.ygol.status:
                msg = u'batch_job_details表里更新列 - SQL2：%s - SQL3: %s -' \
                      u' 更新数据失败。 -  Msg: %s' % \
                      (sql2, sql3, self.dtconn.ygol.msg)
                logger.error(msg)
                result = dict(status=500, msg=msg)
        self.dtconn.ygol.transaction_commit_and_close()
        return result

    def update_PeriodicTask(self):
        # 更新PeriodicTask
        obj = PeriodicTask.objects.get(args="[%s]" % self.batch_job_id)
        for k, v in self.schedule_dict.items():
            setattr(obj, k, v)
        obj.save()
        return obj

    def get_batch_job_by_id(self, batch_job_id):
        # 根据批处理作业id，获取批处理作业信息
        sql = config.query_batch_job_sql3 % batch_job_id
        source_data = self.dtconn.ygol.getsingle(sql)
        return self.dtsf.get_row_by_dict_to_user(source_data)

    def get_batch_job_details_by_id(self, batch_job_id):
        # 根据批处理作业id，获取批处理作业子作业batch_job_details表信息
        sql = config.query_batch_job_sub_job_by_id_sql % batch_job_id
        source_data = self.dtconn.ygol.getall(sql)
        return map(self.dtsf.get_row_by_dict_to_user, source_data)


class BatchJobInstanceData(object):
    """
    批处理作业实例数据处理
    data 为批处理作业更新页面提及的任务数据

    数据格式：
            {u'_id': u'30',
             u'batch_job_details': [{u'batch_job_id': u'30',
                                     u'create_time': u'2018-07-24 21:18:31',
                                     u'description': u'\u6570\u636e\u540c\u6b65\u6d4b\u8bd5',
                                     u'id': u'37',
                                     u'modify_time': u'2018-07-24 21:18:31',
                                     u'name': u'test',
                                     u'subjob_id': u'28',
                                     u'type': u'1'}],
             u'crontab': u'2',
             u'description': u'DIY\u6d4b\u8bd5\u7ec4\u88c5\u673a1',
             u'is_enable': False,
             u'name': u'test1',
             u'operation_type': 2,
             u'task_template': u'celery.chunks',
             u'trigger_mode': 2}

             _id：batch_job_id 批处理作业表 id
    """
    def __init__(self, data):
        self._batch_job_id = data.get('_id', None)
        self.name = data.get('name', '')
        self.description = data.get('description', '')
        self._trigger_mode = data.get('trigger_mode', None)

        self.dtconn = dataconn.DatabaseConnection(logger)
        self.dtsf = dataconn.DataTransform()
        self.dh = DatetimeHelp()
        self.__timestamp1 = self.dh.timestamp1

        self._batch_job_instance_id = None
        self.batch_job_instance_id = self._batch_job_id

    @property
    def batch_job_instance_id(self):
        """
        批处理作业实例ID
        由批处理作业ID + 时间戳组成
        :return: 30 + 1532522114566 = 301532522114566
        """
        return self._batch_job_instance_id

    @batch_job_instance_id.setter
    def batch_job_instance_id(self, batch_job_id):
        self._batch_job_instance_id = '%s%s' % (batch_job_id, self.__timestamp1)

    @property
    def trigger_mode(self):
        return self._trigger_mode

    @trigger_mode.setter
    def trigger_mode(self, val):
        if val not in _TRIGGER_MODE:
            msg = u'触发模式值错误 - trigger_mode：%s - %s' % (val, _TRIGGER_MODE_STR)
            logger.error(msg)
        else:
            self._trigger_mode = val

    def get_insert_batch_job_instance_sql(self):
        return config.insert_batch_job_instance_sql % (
            self.batch_job_instance_id,
            self.name,
            self.description,
            self.trigger_mode
        )

    def get_update_batch_job_instance_by_id_sql(self, result):
        return config.update_batch_job_instance_by_id_sql % (
            _STATUS[1],
            result,
            self.dh.nowtimestrf1,
            self.batch_job_instance_id
        )

    @staticmethod
    def get_select_batch_job_instance_by_id_sql(batch_job_instance_id):
        return config.select_batch_job_instance_by_id_sql % batch_job_instance_id

    def get_batch_job_instance_by_id(self, batch_job_instance_id):
        sql = self.get_select_batch_job_instance_by_id_sql(batch_job_instance_id)
        souce_data = self.dtconn.ygol.getsingle(sql)
        return self.dtsf.get_row_by_dict_to_user(souce_data)

    def start_log(self):
        # 开始记录任务日志到batch_job_instance
        sql = self.get_insert_batch_job_instance_sql()
        self.dtconn.ygol.insert(sql)
        if self.dtconn.ygol.status:
            logger.error(u'记录任务日志到batch_job_instance 失败 - SQL: %s - msg: %s' %
                         (sql, self.dtconn.ygol.msg))

    def record_result_log(self, result):
        # 记录任务执行结果 datax_job_instance
        sql = self.get_update_batch_job_instance_by_id_sql(result)
        self.dtconn.ygol.update(sql)
        if self.dtconn.ygol.status:
            logger.error(u'记录任务执行结果 batch_job_instance 失败 - SQL: %s - msg: %s' %
                         (sql, self.dtconn.ygol.msg))


class BatchJobSubjobInstanceData(object):
    """
    批处理作业中的子任务实例数据处理
    data 格式
     u'batch_job_details': [{u'batch_job_id': u'30',
                         u'create_time': u'2018-07-24 21:18:31',
                         u'description': u'\u6570\u636e\u540c\u6b65\u6d4b\u8bd5',
                         u'id': u'37',
                         u'modify_time': u'2018-07-24 21:18:31',
                         u'name': u'test',
                         u'subjob_id': u'28',
                         u'type': u'1'}],

    """
    def __init__(self, data):
        # id 为 子作业实例ID
        self._subjob_instance_id = None
        self._batch_job_instance_id = None
        self.subjob_id = data.get('subjob_id')
        self.type = data.get('type')

        self.dtconn = dataconn.DatabaseConnection(logger)
        self.dtsf = dataconn.DataTransform()
        self.dh = DatetimeHelp()
        self.__timestamp1 = self.dh.timestamp1

    @property
    def subjob_instance_id(self):
        return self._subjob_instance_id

    @subjob_instance_id.setter
    def subjob_instance_id(self, subjob_instance_id):
        self._subjob_instance_id = subjob_instance_id

    @property
    def batch_job_instance_id(self):
        return self._batch_job_instance_id

    @batch_job_instance_id.setter
    def batch_job_instance_id(self, batch_job_instance_id):
        self._batch_job_instance_id = batch_job_instance_id

    def get_insert_batch_job_instance_details_sql_sql(self, _type):
        return config.insert_batch_job_instance_details_sql % (
            self.batch_job_instance_id,
            self.subjob_instance_id,
            _type
        )

    def start_subjob_log(self, _type):
        # 开始记录子任务日志到batch_job_instance_details
        sql = self.get_insert_batch_job_instance_details_sql_sql(_type)
        self.dtconn.ygol.insert(sql)
        if self.dtconn.ygol.status:
            logger.error(u'记录子任务日志到batch_job_instance_details 失败 - SQL: %s - msg: %s' %
                         (sql, self.dtconn.ygol.msg))

    @classmethod
    def run_sub_job(cls, _type, subjob_id, results, batch_job_instance_id, trigger_mode):
        """
        执行批处理作业中的子作业
        :param _type: 子作业类型
        :param subjob_id: 子作业id
        :param results: 保存所有的子作业执行结果
        :param batch_job_instance_id: 批处理作业实例ID
        :return: None
        """
        result = None

        if _type == _SUBJOB_TYPE[0]:
            dataxjd = DataxJobData({})
            dataxjob_data = dataxjd.get_job_data_by_id(subjob_id)
            dataxjob_writer_column = dataxjd.get_datax_job_writer_column_by_id(subjob_id)
            writer_column_id = [dt['name'] for dt in dataxjob_writer_column]
            dataxjob_data['_id'] = subjob_id
            dataxjob_data['trigger_mode'] = trigger_mode
            dataxjob_data['writer_column_id'] = writer_column_id
            bjsid = cls(dataxjob_data)
            dx = Datax(dataxjob_data)
            dx.jd.create_file(dx.job_json_file, dx.get_job_json())
            dx.jd.start_log()  # 记录datax_job 实例同步日志
            bjsid.batch_job_instance_id = batch_job_instance_id
            bjsid.subjob_instance_id = dx.jd.datax_job_instance_id
            bjsid.start_subjob_log(_type)  # 记录子作业日志
            (status, output) = commands.getstatusoutput(dx.cmd)
            result = 1 if status else 0
            dx.jd.record_result_log(result)

        results.append(result)

    @staticmethod
    def get_select_sub_job_datax_instance_by_id_sql(batch_job_instance_id):
        return config.select_sub_job_datax_instance_by_id_sql % batch_job_instance_id

    @staticmethod
    def get_sub_job_datax_instance_data_by_id(batch_job_instance_id):
        dtconn = dataconn.DatabaseConnection(logger)
        dtsf = dataconn.DataTransform()
        sql = BatchJobSubjobInstanceData.get_select_sub_job_datax_instance_by_id_sql(batch_job_instance_id)
        source_data = dtconn.ygol.getall(sql)
        return map(dtsf.get_row_by_dict_to_user, source_data)


class CheckBatchJob(object):
    """
    检测新增、更新、手工运行批处理作业提交的信息
    :return  result
             格式：   {'status': 1, 'msg': '操作类型错误'}
    total_check 启动所有检测，返回检测状态和错误消息
    """
    _SUCCESS = _SUCCESS.copy()
    _OPERATION_TYPE_ERROR1 = dict(status=1, msg=u'操作类型不能为空')
    _OPERATION_TYPE_ERROR2 = dict(status=2, msg=u'操作类型错误')
    _DESCRIPTION_ERROR1 = dict(status=3, msg=u'任务描述不能为空')
    _NAME_ERROR1 = dict(status=4, msg=u'任务名称不能为空')
    _NAME_ERROR2 = dict(status=5, msg=u'任务名称已存在')
    _TASK_TEMPLATE_ERROR1 = dict(status=6, msg=u'任务模板不能为空')
    _TASK_TEMPLATE_ERROR2 = dict(status=6, msg=u'任务模板不存在')
    _IS_ENABLE_ERROR1 = dict(status=7, msg=u'是否启用值错误')
    _BATCH_JOB_DETAILS_ERROR1 = dict(status=8, msg=u'批处理作业详情不能为空')
    _BATCH_JOB_DETAILS_ERROR2 = dict(status=9, msg=u'批处理作业详情，子作业 %s %s 类型错误')
    _BATCH_JOB_DETAILS_ERROR3 = dict(status=10, msg=u'批处理作业详情，子作业 %s %s ID %s 不存在')
    _TRIGGER_MODE_ERROR1 = dict(status=11, msg=u'触发模式 不存在')
    _TRIGGER_MODE_ERROR2 = dict(status=12, msg=u'触发模式值错误')
    _CRONTAB_ERROR = dict(status=13, msg=u'执行时间错误')
    _BATCH_JOB_ID_ERROR1 = dict(status=14, msg=u'batch_job_id 不能为空')
    _BATCH_JOB_ID_ERROR2 = dict(status=15, msg=u'batch_job_id 不存在')

    def __init__(self, request):
        cur = Currency(request)
        data = cur.rq_post_json('data')
        self.dtconn = dataconn.DatabaseConnection(logger)
        self.jd = BatchJobData(data)

        self.error_msg = []
        self.result = self._SUCCESS

    def check_operation_type(self):
        # 检测操作类型
        operation_type = self.jd.operation_type
        if not operation_type:
            self.result = self._OPERATION_TYPE_ERROR1
        else:
            if operation_type not in _OPERATION_TYPE:
                self.result = self._OPERATION_TYPE_ERROR2

    def check_name_by_operation_type(self):
        # 根据操作类型 检测任务名称
        name = self.jd.name
        if self.jd.operation_type == _OPERATION_TYPE[0]:
            sql = config.query_batch_job_by_name_sql % name
            self.check_name(name, sql)

        if self.jd.operation_type == _OPERATION_TYPE[1]:
            sql = config.query_batch_job_sql1 % (name, self.jd.batch_job_id)
            self.check_name(name, sql)

    def check_name(self, name, sql):
        # 新增、更新、运行批处理作业时，检测名称
        if name:
            data = self.dtconn.ygol.getsingle(sql)
            if self.dtconn.ygol.status:
                _msg = u'检测任务名称时数据库错误。 - Msg: %s' % self.dtconn.ygol.msg
                logger.error(_msg)
                self.result = dict(status=500, msg=_msg)
            else:
                if data:
                    self.result = self._NAME_ERROR2
        else:
            self.result = self._NAME_ERROR1

    def check_description(self):
        # 检测任务描述
        description = self.jd.description
        if not description:
            self.result = self._DESCRIPTION_ERROR1

    def check_task_template(self):
        # 检测任务模板
        loaders.autodiscover()
        tasks = list(sorted(registry.tasks.regular().keys()))
        if self.jd.task_template:
            if self.jd.task_template not in tasks:
                self.result = self._TASK_TEMPLATE_ERROR1
        else:
            self.result = self._TASK_TEMPLATE_ERROR2

    def check_is_enable(self):
        # 检测“是否启用”
        is_enable = self.jd.is_enable
        if not isinstance(is_enable, bool):
            self.result = self._IS_ENABLE_ERROR1

    def check_crontab(self):
        crontab = self.jd.crontab
        crons = CrontabSchedule.objects.values('id')
        try:
            if long(crontab) not in [c['id'] for c in crons]:
                self.result = self._CRONTAB_ERROR
        except Exception, e:
            self.result = self._CRONTAB_ERROR

    def check_batch_job_details(self):
        """
        检查批处理作业详情
        先验证子作业类型，再验证子作业是否存在
        :return:
        """
        data = self.jd.batch_job_details
        if data:
            for dt in data:
                try:
                    _type = int(dt['type'])
                except Exception as e:
                    msg = self._BATCH_JOB_DETAILS_ERROR2.get('msg') % (dt['name'], dt['description'])
                    status = self._BATCH_JOB_DETAILS_ERROR2.get('status')
                    self.result = dict(status=status, msg=msg)
                    break
                else:
                    if _type not in _SUBJOB_TYPE:
                        msg = self._BATCH_JOB_DETAILS_ERROR2.get('msg') % (dt['name'], dt['description'])
                        status = self._BATCH_JOB_DETAILS_ERROR2.get('status')
                        self.result = dict(status=status, msg=msg)
                        break
                    else:
                        msg = self._BATCH_JOB_DETAILS_ERROR3.get('msg') % (dt['name'], dt['description'], dt['subjob_id'])
                        status = self._BATCH_JOB_DETAILS_ERROR3.get('status')
                        # 数据同步
                        if _type == _SUBJOB_TYPE[0]:
                            sql = datax_web_config.query_datax_job_by_id_sql % dt['subjob_id']
                            data = self.dtconn.ygol.getsingle(sql)
                            if self.dtconn.ygol.status:
                                _msg = u'检测datax_job_id 错误 - SQL: %s。 - Msg: %s' % (sql, self.dtconn.ygol.msg)
                                logger.error(_msg)
                                self.result = dict(status=500, msg=_msg)
                            else:
                                if not data:
                                    self.result = dict(status=status, msg=msg)
        else:
            self.result = self._BATCH_JOB_DETAILS_ERROR1

    def check_batch_job_id(self):
        # 检测批处理作业ID
        if self.jd.operation_type == _OPERATION_TYPE[1]:
            _id = self.jd.batch_job_id
            if isinstance(_id, _str) and _id and _id.isdigit():
                sql = config.query_batch_job_sql3 % _id
                data = self.dtconn.ygol.getsingle(sql)
                if self.dtconn.ygol.status:
                    _msg = u'检测batch_job_id 错误 - SQL: %s。 - Msg: %s' % (sql, self.dtconn.ygol.msg)
                    logger.error(_msg)
                    self.result = dict(status=500, msg=_msg)
                else:
                    if not data:
                        self.result = self._BATCH_JOB_ID_ERROR2
            else:
                self.result = self._BATCH_JOB_ID_ERROR1

    def check_trigger_mode(self):
        # 检测触发模式
        trigger_mode = self.jd.trigger_mode
        if self.jd.operation_type == _OPERATION_TYPE[1]:
            if not trigger_mode:
                self.result = self._TRIGGER_MODE_ERROR1
            else:
                if trigger_mode not in _TRIGGER_MODE:
                    self.result = self._TRIGGER_MODE_ERROR2


    def total_check(self):
        check_func = ['check_operation_type', 'check_name_by_operation_type',
                      'check_description', 'check_trigger_mode', 'check_task_template',
                      'check_is_enable', 'check_crontab', 'check_batch_job_details',
                      'check_batch_job_id'
                      ]

        for func_name in check_func:
            getattr(self, func_name)()
            if self.result['status']:
                break

        return self.result


class BatchJobInstanceSql(object):
    # datax job instance 查询sql
    _table_bji = {
        'name': {'data_type': 'str', 'val': ''},
        'description': {'data_type': 'str', 'val': ''},
        'status': {'data_type': 'str', 'val': ''},
        'result': {'data_type': 'str', 'val': ''},
        'trigger_mode': {'data_type': 'str', 'val': ''},
    }

    _order_by = [{'table': 'bji', 'field': 'start_time', 'rule': 'DESC'}]

    def __init__(self, request):
        self.cur = Currency(request)
        self.rq_get = self.cur.rq_get

        self._offset = int(self.rq_get('offset'))
        self._limit = int(self.rq_get('limit'))
        self._SQL = config.select_batch_job_instance_sql
        self._TOTAL_SQL = config.count_batch_job_instance_sql
        self._set_table(self._table_bji)

    def _set_table(self, table):
        for field, attr in table.items():
            val = self.rq_get(field)
            attr['val'] = val
        return table

    @property
    def tables(self):
        _tables = {'bji': self._table_bji}
        return _tables

    @property
    def cvtpara(self):
        _cvtpara = {
            'offset': self._offset,
            'limit': self._limit,
            'sql': self._SQL,
            'total_sql': self._TOTAL_SQL,
            'order_by': self._order_by,
            'order_rule': self._order_by
        }
        return _cvtpara


class PaginatorBatchJobInstance(dataconn.DatabaseConnection, query_sql.Q_Data):
    # 分页访问数据
    def __init__(self, qs):
        super(PaginatorBatchJobInstance, self).__init__(logger)
        query_sql.Q_Data.__init__(self, qs)

    @property
    def rows(self):
        return self._get_rows(self.ygol)

    @property
    def total(self):
        return self._get_total(self.ygol)


@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
def index(request):
    # 批处理作业首页
    return render(request, 'batch_job/index.html', locals())

@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
@permission_required('batch_job.editBatchJob', raise_exception=PermissionDenied)
def add_batch_job(request):
    # 新建批处理作业
    return render(request, 'batch_job/add_batch_job.html', locals())

@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
def update_batch_job(request, id):
    # 更新批处理作业
    return render(request, 'batch_job/update_batch_job.html', locals())

@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
def batch_job_instance(request):
    # 批处理作业执行历史
    return render(request, 'batch_job/batch_job_instance.html', locals())


@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
def batch_job_instance_details(request, id):
    # 批处理作业详情执行历史
    return render(request, 'batch_job/batch_job_instance_details.html', locals())


@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
def get_task_template(request):
    # 任务模板
    irrelevant_tasks = ['FirstBlood.celery.debug_task',
                         'celery.backend_cleanup',
                         'celery.chain',
                         'celery.chord',
                         'celery.chord_unlock',
                         'celery.chunks',
                         'celery.group',
                         'celery.map',
                         'celery.starmap',
                         'run',
                         u'run_batch_job']

    loaders.autodiscover()
    response = HttpResponse()
    tasks = list(sorted(registry.tasks.regular().keys()))
    for t in irrelevant_tasks:
        tasks.remove(t)
    response.write(json.dumps(tasks))
    return response


@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
def get_crontab(request):
    # 获取 crontab 定时时间
    response = HttpResponse()
    data = CrontabSchedule.objects.values()
    response.write(json.dumps(list(data)))
    return response


@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
@permission_required('batch_job.editBatchJob', raise_exception=PermissionDenied)
def add_crontab(request):
    # 新增 crontab 定时时间
    response = HttpResponse()
    cur = Currency(request)
    rq_post = getattr(cur, 'rq_post')
    jdata = rq_post('data')
    data = json.loads(jdata)
    ndata = dict([(k, v.replace(' ', '')) for k, v in data.items()])  # Remove all spaces
    crobj = schedules.crontab(**ndata)
    to_model_schedule = ModelEntry.to_model_schedule
    model_schedule, model_field = to_model_schedule(crobj)
    response.write(json.dumps(ndata))
    return response


@login_required
@verification(CheckBatchJob)
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
@permission_required('batch_job.editBatchJob', raise_exception=PermissionDenied)
def add_batch_job_data(request):
    # 新增或者修改任务数据
    response = HttpResponse()
    cur = Currency(request)
    data = cur.rq_post_json('data')
    jd = BatchJobData(data)

    if jd.operation_type == _OPERATION_TYPE[0]:
        result = jd.create()  # 新批处理作业
        jd.create_PeriodicTask()  # 创建定时任务
    else:
        result = jd.update()
        jd.update_PeriodicTask()
    response.write(json.dumps(result))
    return response


@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
def get_batch_job_data(request):
    # 获取批处理作业数据
    sql = config.query_batch_job_sql2
    dtconn = dataconn.DatabaseConnection(logger)
    dtsf = dataconn.DataTransform()
    source_data = dtconn.ygol.getall(sql)
    data = [dtsf.get_row_by_dict_to_user(dt) for dt in source_data]
    response = HttpResponse()
    response.write(json.dumps(data))
    return response


@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
def get_batch_job_data_by_id(request):
    """
    根据ID获取批处理作业数据
    :param request: id
    :return:
    """
    cur = Currency(request)
    _id = cur.rq_post('_id')
    sql = config.query_batch_job_sql3 % _id
    dtconn = dataconn.DatabaseConnection(logger)
    dtsf = dataconn.DataTransform()
    source_data = dtconn.ygol.getsingle(sql)
    response = HttpResponse()
    response.write(json.dumps(dtsf.get_row_by_dict_to_user(source_data)))
    return response


@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
def get_batch_job_sub_job_by_id(request):
    """
    根据ID获取批处理作业中的子作业数据

    扩展：
         目前子作业只包含数据同步，后期加入了SQL脚本、SQL备份等等之后，
         需要先判断子作业的类型，再根据类型去相关表里查询子作业的详细信息。
         例如：如果有同步类型的子作业，就需要根据同步作业表查询同步的任务详情。
              如果有备份类型的，就去备份表里查询备份任务详情。
    :param request: id
    :return:
    """
    cur = Currency(request)
    _id = cur.rq_post('_id')
    sql = config.query_batch_job_sub_job_by_id_sql % _id
    dtconn = dataconn.DatabaseConnection(logger)
    dtsf = dataconn.DataTransform()
    source_data = dtconn.ygol.getall(sql)
    data = [dtsf.get_row_by_dict_to_user(dt) for dt in source_data]
    response = HttpResponse()
    response.write(json.dumps(data))
    return response


@login_required
@verification(CheckBatchJob)
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
@permission_required('batch_job.editBatchJob', raise_exception=PermissionDenied)
def run_batch_job_task(request):
    # 执行批处理作业
    response = HttpResponse()
    cur = Currency(request)
    rq_post = getattr(cur, 'rq_post')
    jdata = rq_post('data')
    data = json.loads(jdata)
    # run_batch_job(**data)
    run_batch_job.delay(**data)
    response.write(json.dumps({'status': 0, 'msg': u'操作成功'}))
    return response


def _process_run_sub_job(_type, subjob_id, results, batch_job_instance_id, trigger_mode):
    # 多进程执行批处理任务的子任务
    BatchJobSubjobInstanceData.run_sub_job(_type, subjob_id, results, batch_job_instance_id, trigger_mode)


@shared_task(name='run_batch_job')
def run_batch_job(**data):
    """
    异步执行批处理作业任务
    :param data:
            {u'_id': u'30',
             u'batch_job_details': [{u'batch_job_id': u'30',
                                     u'create_time': u'2018-07-24 21:18:31',
                                     u'description': u'\u6570\u636e\u540c\u6b65\u6d4b\u8bd5',
                                     u'id': u'37',
                                     u'modify_time': u'2018-07-24 21:18:31',
                                     u'name': u'test',
                                     u'subjob_id': u'28',
                                     u'type': u'1'}],
             u'crontab': u'2',
             u'description': u'DIY\u6d4b\u8bd5\u7ec4\u88c5\u673a1',
             u'is_enable': False,
             u'name': u'test1',
             u'operation_type': 2,
             u'task_template': u'celery.chunks',
             u'trigger_mode': 2}

             _id：batch_job_id 批处理作业表 id
    :return:
    """
    bjid = BatchJobInstanceData(data)
    bjid.start_log()

    curr_proc = mp.current_process()
    # celery 里执行任务时，默认守护进程无法开启多进程，需要先将当前进程设置为非守护进程, 启动执行再改为守护进程
    curr_proc.daemon = False
    manager = mp.Manager()
    results = manager.list()  # 记录所有子作业的执行结果
    p = mp.Pool(config.maxtasksperchild)
    curr_proc.daemon = True

    batch_job_details = data.get('batch_job_details')
    for sj in batch_job_details:
        _type = int(sj.get('type'))
        subjob_id = sj.get('subjob_id')
        p.apply_async(_process_run_sub_job, args=(_type, subjob_id, results,
                                                  bjid.batch_job_instance_id,
                                                  bjid.trigger_mode))

    p.close()
    p.join()
    batch_job_result = 0 if 1 not in results else 1
    bjid.record_result_log(batch_job_result)


@shared_task(name='batch_job_periodictask')
def batch_job_periodictask(batch_job_id):
    """
    定时执行批处理作业
                        通过batch_job_id查询批处理作业的信息来执行任务。

    :param batch_job_id: 批处理作业ID，也就是batch_job表id
    :return: None
    """
    bjd = BatchJobData({})
    batch_job_data = bjd.get_batch_job_by_id(batch_job_id)

    bjid = BatchJobInstanceData(batch_job_data)
    bjid.batch_job_instance_id = batch_job_id
    bjid.trigger_mode = _TRIGGER_MODE[0]  # 触发模式：自动
    bjid.start_log()

    curr_proc = mp.current_process()
    # celery 里执行任务时，默认守护进程无法开启多进程，需要先将当前进程设置为非守护进程, 启动执行后再改为守护进程
    curr_proc.daemon = False
    manager = mp.Manager()
    results = manager.list()  # 记录所有子作业的执行结果
    p = mp.Pool(config.maxtasksperchild)
    curr_proc.daemon = True

    batch_job_details = bjd.get_batch_job_details_by_id(batch_job_id)
    for sj in batch_job_details:
        _type = int(sj.get('type'))
        subjob_id = sj.get('subjob_id')
        p.apply_async(_process_run_sub_job, args=(_type, subjob_id, results,
                                                  bjid.batch_job_instance_id,
                                                  bjid.trigger_mode))

    p.close()
    p.join()
    batch_job_result = 0 if 1 not in results else 1
    bjid.record_result_log(batch_job_result)


# @login_required
# @permission_required('change.view_delivery', raise_exception=PermissionDenied)
# @permission_required('change.edit_delivery', raise_exception=PermissionDenied)
def get_batch_job_instance(request):
    # 分页查询批处理作业实例
    dsql = BatchJobInstanceSql(request)
    cvtpara = dsql.cvtpara
    tables = dsql.tables
    qs = query_sql.Q_Sql(cvtpara, **tables)
    pd = PaginatorBatchJobInstance(qs)
    response = HttpResponse()
    response.write(json.dumps({'rows': pd.rows, 'total': pd.total}))
    return response


@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
def get_batch_job_instance_data_by_id(request):
    # 根据ID获取批处理作业数据
    cur = Currency(request)
    batch_job_instance_id = cur.rq_post('instance_id')
    bjid = BatchJobInstanceData({})
    data = bjid.get_batch_job_instance_by_id(batch_job_instance_id)
    response = HttpResponse()
    response.write(json.dumps(data))
    return response


@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
def get_batch_job_sub_job_instance_data_by_id(request):
    # 根据ID获取批处理作业数据
    cur = Currency(request)
    batch_job_instance_id = cur.rq_get('instance_id')
    # if _type == 1: 数据同步类型为1
    data = BatchJobSubjobInstanceData.get_sub_job_datax_instance_data_by_id(batch_job_instance_id)
    response = HttpResponse()
    response.write(json.dumps(data))
    return response