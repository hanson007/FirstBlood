# -*- coding: UTF-8 -*-
from django.contrib.auth.decorators import permission_required
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.shortcuts import render
from django.http import HttpResponse
from functools import wraps
from controller.core.public import (Currency, DatetimeHelp)
from controller.core import query_sql
from controller.public import dataconn
from celery import shared_task
from conf import config
import commands
import logging
import sys
import json
reload(sys)
sys.setdefaultencoding("utf-8")


logger = logging.getLogger('datax_web')
_SUCCESS = dict(status=0, msg=u'检测成功')
_str = (str, unicode)

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
            result = ccl.total_check()
            if result['status']:
                response.write(json.dumps(result))
                return response

            return view_func(request, *args, **kwargs)
        return _wrapped_view
    return decorator


class JobData(object):
    """
    新增或更新定时任务时处理数据
    data 数据格式

    {u'_id': u'28',
     u'description': u'\u6570\u636e\u540c\u6b65\u6d4b\u8bd5',
     u'name': u'test',
     u'operation_type': u'mod',
     u'querySql': u'select * from `admin-service`.as_user_info limit 10;',
     u'reader_databaseinfo_id': u'1',
     u'trigger_mode': 2,
     u'writer_column_id': [u'*'],
     u'writer_databaseinfo_id': u'22',
     u'writer_postSql': u'',
     u'writer_preSql': u'truncate table `admin-service`.as_user_info;',
     u'writer_table': u'`admin-service`.as_user_info'}

     _id: datax_job_id
    """
    def __init__(self, data):
        # id 为 datax_job_id
        self.id = data.get('_id', 0)
        self.name = data.get('name', '')
        self.description = data.get('description', '')
        self.querySql = data.get('querySql', '')
        self.reader_databaseinfo_id = data.get('reader_databaseinfo_id', '')
        self.writer_table = data.get('writer_table', '')
        self.writer_column = data.get('writer_column_id', [])
        self.writer_databaseinfo_id = data.get('writer_databaseinfo_id', '')
        self.writer_preSql = data.get('writer_preSql', '')
        self.writer_postSql = data.get('writer_postSql', '')
        self.operation_type = data.get('operation_type', '')
        self.trigger_mode = data.get('trigger_mode', '')

        self.dtconn = dataconn.DatabaseConnection(logger)
        self.dtsf = dataconn.DataTransform()
        self.dh = DatetimeHelp()
        self.__timestamp1 = self.dh.timestamp1

        self.reader_dtbs = self._get_reader_dtbs() if self.reader_databaseinfo_id else None
        self.writer_dtbs = self._get_writer_dtbs() if self.writer_databaseinfo_id else None

    @property
    def timestamp1(self):
        return self.__timestamp1

    def _get_reader_dtbs(self):
        return self.dtconn.get_datainfo_by_id(self.reader_databaseinfo_id)

    def _get_writer_dtbs(self):
        return self.dtconn.get_datainfo_by_id(self.writer_databaseinfo_id)

    def get_insert_datax_job_sql(self):
        # 在 datax_job表里创建新的任务  -  新增SQL
        querySql = self.dtsf.special_characters_mysql(self.querySql)
        writer_preSql = self.dtsf.special_characters_mysql(self.writer_preSql)
        writer_postSql = self.dtsf.special_characters_mysql(self.writer_postSql)

        return config.insert_datax_job_sql % (
            self.name, self.description, querySql, self.reader_databaseinfo_id,
            self.writer_table, self.writer_databaseinfo_id, writer_preSql, writer_postSql
        )

    def get_update_datax_job_by_id_sql(self):
        # 在 datax_job表里更新任务  - 更新SQL
        querySql = self.dtsf.special_characters_mysql(self.querySql)
        writer_preSql = self.dtsf.special_characters_mysql(self.writer_preSql)
        writer_postSql = self.dtsf.special_characters_mysql(self.writer_postSql)

        return config.update_datax_job_by_id_sql % (
            self.name, self.description, querySql, self.reader_databaseinfo_id,
            self.writer_table, self.writer_databaseinfo_id, writer_preSql, writer_postSql,
            self.id
        )

    def get_insert_datax_job_writer_column_sql(self):
        # 拼接写入列SQL  insert into  values ('user_id', 1), ('card_name', 1)
        datax_job_id = self.id or self.dtconn.ygol.insert_id
        values_list =   ["('%s', %s)" % (column, datax_job_id) for column in self.writer_column]
        return config.insert_datax_job_writer_column_sql + ','.join(values_list)

    def get_delete_datax_job_writer_column_by_id_sql(self):
        # 获取删除写入列SQL ， 根据ID删除
        return config.delete_datax_job_writer_column_by_id_sql % self.id

    def get_insert_datax_job_instance_sql(self):
        querySql = self.dtsf.special_characters_mysql(self.querySql)
        writer_preSql = self.dtsf.special_characters_mysql(self.writer_preSql)
        writer_postSql = self.dtsf.special_characters_mysql(self.writer_postSql)

        return config.insert_datax_job_instance_sql % (
            self.datax_job_instance_id,
            self.name,
            self.description,
            querySql,
            self.reader_dtbs['host'],
            self.reader_dtbs['description'],
            self.writer_table,
            self.writer_dtbs['host'],
            self.writer_dtbs['description'],
            self.trigger_mode,
            writer_preSql,
            writer_postSql
        )

    def get_update_datax_job_instance_by_instance_id_sql(self, result):
        return config.update_datax_job_instance_by_instance_id_sql % (
            1, result, self.dh.now_time, self.datax_job_instance_id
        )

    @property
    def datax_job_instance_id(self):
        return '%s%s' % (self.id, self.__timestamp1)

    def start_log(self):
        # 开始记录任务日志到datax_job_instance
        sql = self.get_insert_datax_job_instance_sql()
        self.dtconn.ygol.insert(sql)
        if self.dtconn.ygol.status:
            logger.error(u'记录任务日志到datax_job_instance 失败 - SQL: %s - msg: %s' %
                         (sql, self.dtconn.ygol.msg))

    def record_result_log(self, result):
        # 记录任务执行结果 datax_job_instance
        sql = self.get_update_datax_job_instance_by_instance_id_sql(result)
        self.dtconn.ygol.update(sql)
        if self.dtconn.ygol.status:
            logger.error(u'记录任务执行结果 datax_job_instance 失败 - SQL: %s - msg: %s' %
                         (sql, self.dtconn.ygol.msg))

    def create(self):
        # datax_job表里创建新的任务
        result = _SUCCESS.copy()
        sql1 = self.get_insert_datax_job_sql()
        self.dtconn.ygol.transaction_start()
        self.dtconn.ygol.transaction_execute(sql1)
        if self.dtconn.ygol.status:
            msg = u'datax_job表里创建新的任务，SQL：%s 插入数据失败。 -  Msg: %s' % \
                  (sql1, self.dtconn.ygol.msg)
            logger.error(msg)
            result = dict(status=500, msg=msg)
        else:
            sql2 = self.get_insert_datax_job_writer_column_sql()
            self.dtconn.ygol.transaction_execute(sql2)
            if self.dtconn.ygol.status:
                msg = u'datax_job_writer_column表里创建新的列，SQL：%s 插入数据失败。 -  Msg: %s' % \
                      (sql2, self.dtconn.ygol.msg)
                logger.error(msg)
                result = dict(status=500, msg=msg)
        self.dtconn.ygol.transaction_commit_and_close()
        return result

    def update(self):
        # 更新任务
        result = _SUCCESS.copy()
        sql1 = self.get_update_datax_job_by_id_sql()
        self.dtconn.ygol.transaction_start()
        self.dtconn.ygol.transaction_execute(sql1)
        if self.dtconn.ygol.status:
            msg = u'datax_job表，SQL：%s 更新数据失败。 -  Msg: %s' % \
                  (sql1, self.dtconn.ygol.msg)
            logger.error(msg)
            result = dict(status=500, msg=msg)
        else:
            sql2 = self.get_delete_datax_job_writer_column_by_id_sql()
            sql3 = self.get_insert_datax_job_writer_column_sql()
            self.dtconn.ygol.transaction_execute(sql2)
            self.dtconn.ygol.transaction_execute(sql3)
            if self.dtconn.ygol.status:
                msg = u'datax_job_writer_column表里更新列 - SQL2：%s - SQL3: %s -' \
                      u' 更新数据失败。 -  Msg: %s' % \
                      (sql2, sql3, self.dtconn.ygol.msg)
                logger.error(msg)
                result = dict(status=500, msg=msg)
        self.dtconn.ygol.transaction_commit_and_close()
        return result

    def get_job_data(self):
        # 获取任务数据
        source_data = self.dtconn.ygol.getall(config.query_datax_job_sql)
        if self.dtconn.ygol.status:
            logger.error(u'获取datax_job信息失败 %s' % self.dtconn.ygol.msg)
            return []
        else:
            return [self.dtsf.get_row_by_dict_to_user(dt) for dt in source_data]

    def get_job_data_by_id(self, _id):
        """
        根据ID获取任务数据
        :param _id: datax_job_id
        :return: datax_job
        """
        source_data = self.dtconn.ygol.getsingle(config.query_datax_job_by_id_sql % _id)
        if self.dtconn.ygol.status:
            logger.error(u'根据ID %s 获取任务数据信息失败 %s' % (self.id, self.dtconn.ygol.msg))
            return None
        else:
            return self.dtsf.get_row_by_dict_to_user(source_data)


    def get_datax_job_writer_column_by_id(self, _id):
        """
        根据ID获取任务需要写入的列
        :param _id: datax_job_id
        :return: datax_job_writer_column
        """
        source_data = self.dtconn.ygol.getall(config.query_datax_job_writer_column_by_id_sql % _id)
        if self.dtconn.ygol.status:
            logger.error(u'根据ID %s 获取任务数据信息失败 %s' % (self.id, self.dtconn.ygol.msg))
            return None
        else:
            return [self.dtsf.get_row_by_dict_to_user(dt) for dt in source_data]

    @staticmethod
    def create_file(file, content):
        # 创建文件
        with open(file, 'w') as f:
            f.write(content)


class CheckJob(object):
    """
    检测新增任务提交的信息
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
    _QUERY_SQL_ERROR1 = dict(status=6, msg=u'查询SQL语句不能为空')
    _READER_DATABASEINFO_ID_ERROR1 = dict(status=7, msg=u'读取数据库不能为空，必须为数字')
    _READER_DATABASEINFO_ID_ERROR2 = dict(status=8, msg=u'读取数据库ID不存在')
    _WRITER_TABLE_ERROR1 = dict(status=10, msg=u'写入表不能为空')
    _WRITER_COLUMN_ERROR1 = dict(status=11, msg=u'写入列不能为空')
    _WRITER_DATABASEINFO_ID_ERROR1 = dict(status=12, msg=u'写入数据库不能为空')
    _WRITER_DATABASEINFO_ID_ERROR2 = dict(status=13, msg=u'写入数据库ID不存在')
    _DATAX_JOB_ID_ERROR1 = dict(status=1, msg=u'datax_job_id 不能为空')
    _DATAX_JOB_ID_ERROR2 = dict(status=2, msg=u'datax_job_id 不存在')
    _TRIGGER_MODE_ERROR1 = dict(status=2, msg=u'触发模式 不存在')
    _TRIGGER_MODE_ERROR2 = dict(status=2, msg=u'触发模式值错误')


    def __init__(self, request):

        """
        RESPONSE_TYPE 返回给用户数据的方式
        1：20万行以内的数据，以excel方式返回
        2：超过20万行的数据，需要分批处理
        3：小量的数据以HTML表格的方式返回'
        """
        cur = Currency(request)
        data = cur.rq_post_json('data')
        self.dtconn = dataconn.DatabaseConnection(logger)
        self.jd = JobData(data)

        self.error_msg = []
        self.result = self._SUCCESS

    def check_operation_type(self):
        # 检测操作类型
        operation_type = self.jd.operation_type
        if not operation_type:
            self.result = self._OPERATION_TYPE_ERROR1
        else:
            if operation_type not in config.OPERATION_TYPE:
                self.result = self._OPERATION_TYPE_ERROR2

    def check_name_by_operation_type(self):
        # 根据操作类型add/mod 检测任务名称
        name = self.jd.name
        if self.jd.operation_type == config.OPERATION_TYPE[0]:
            sql = config.query_datax_job_by_name_sql % name
            self.check_name(name, sql)

        if self.jd.operation_type == config.OPERATION_TYPE[1]:
            sql = config.query_datax_job_sql2 % (name, self.jd.id)
            self.check_name(name, sql)

    def check_name(self, name, sql):
        # 修改任务时，检测任务名称
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

    def check_querySql(self):
        # 检测查询SQL语句
        querySql = self.jd.querySql
        if not querySql:
            self.result = self._DESCRIPTION_ERROR1

    def check_reader_databaseinfo_id(self):
        # 检测读取数据库
        kwargs = {
            '_id': self.jd.reader_databaseinfo_id,
            'operation_type': u'读取',
            'ERROR1': self._READER_DATABASEINFO_ID_ERROR1,
            'ERROR2': self._READER_DATABASEINFO_ID_ERROR2,
        }
        self.check_databaseinfo_id(**kwargs)

    def check_writer_table(self):
        # 检测写入表
        writer_table = self.jd.writer_table
        if not writer_table:
            self.result = self._WRITER_TABLE_ERROR1

    def check_writer_column(self):
        # 检测写入列
        writer_column = self.jd.writer_column
        if not writer_column:
            self.result = self._WRITER_COLUMN_ERROR1

    def check_writer_databaseinfo_id(self):
        # 检测写入数据库
        kwargs = {
            '_id': self.jd.writer_databaseinfo_id,
            'operation_type': u'写入',
            'ERROR1': self._WRITER_DATABASEINFO_ID_ERROR1,
            'ERROR2': self._WRITER_DATABASEINFO_ID_ERROR2,
        }
        self.check_databaseinfo_id(**kwargs)

    def check_databaseinfo_id(self, _id, operation_type, ERROR1, ERROR2):
        # 检测数据库ID
        if _id and _id.isdigit():
            data = self.dtconn.get_datainfo_by_id(int(_id))
            if self.dtconn.ygol.status:
                _msg = u'检测%s数据库错误。 - Msg: %s' % (operation_type, self.dtconn.ygol.msg)
                logger.error(_msg)
                self.result = dict(status=500, msg=_msg)
            else:
                if not data:
                    self.result = ERROR2
        else:
            self.result = ERROR1

    def check_datax_job_id(self):
        # 检测任务ID
        if self.jd.operation_type == config.OPERATION_TYPE[1]:
            _id = self.jd.id
            if isinstance(_id, _str) and _id and _id.isdigit():
                sql = config.query_datax_job_by_id_sql % _id
                data = self.dtconn.ygol.getsingle(sql)
                if self.dtconn.ygol.status:
                    _msg = u'检测datax_job_id 错误 - SQL: %s。 - Msg: %s' % (sql, self.dtconn.ygol.msg)
                    logger.error(_msg)
                    self.result = dict(status=500, msg=_msg)
                else:
                    if not data:
                        self.result = self._DATAX_JOB_ID_ERROR2
            else:
                self.result = self._DATAX_JOB_ID_ERROR1


    def check_trigger_mode(self):
        # 检测触发模式
        trigger_mode = self.jd.trigger_mode
        if self.jd.operation_type == config.OPERATION_TYPE[1]:
            if not trigger_mode:
                self.result = self._TRIGGER_MODE_ERROR1
            else:
                if trigger_mode not in config.TRIGGER_MODE:
                    self.result = self._TRIGGER_MODE_ERROR2


    def total_check(self):
        check_func = ['check_operation_type', 'check_datax_job_id',
                      'check_description', 'check_name_by_operation_type',
                      'check_querySql', 'check_reader_databaseinfo_id',
                      'check_writer_table', 'check_writer_column',
                      'check_writer_databaseinfo_id', 'check_trigger_mode'
                      ]

        for func_name in check_func:
            getattr(self, func_name)()
            if self.result['status']:
                break

        return self.result


class Datax(object):
    """
    处理和datax相关的操作
    """
    def __init__(self, data):
        self.jd = JobData(data)

    def get_reader(self):
        return dict(
            jdbcUrl = 'jdbc:mysql://%s/%s' % (self.jd.reader_dtbs['host'], self.jd.reader_dtbs['db']),
            querySql = self.jd.querySql,
            password = self.jd.reader_dtbs['passwd'],
            username = self.jd.reader_dtbs['user']
        )

    def get_writer(self):
        return dict(
            column = "[%s]" % ','.join(['"%s"' % str(c) for c in self.jd.writer_column]),
            jdbcUrl = 'jdbc:mysql://%s/%s' % (self.jd.writer_dtbs['host'], self.jd.writer_dtbs['db']),
            table = self.jd.writer_table,
            password = self.jd.writer_dtbs['passwd'],
            preSql = ','.join(map(lambda x:'"%s"' % x, self.jd.writer_preSql.split(';'))),
            postSql = ','.join(map(lambda x:'"%s"' % x, self.jd.writer_postSql.split(';'))),
            username = self.jd.writer_dtbs['user']
        )

    def get_job_json(self):
        reader = self.get_reader()
        writer = self.get_writer()
        return config.datax_job_template % (
            reader['jdbcUrl'], reader['querySql'], reader['password'], reader['username'],
            writer['column'], writer['jdbcUrl'], writer['table'], writer['password'],
            writer['preSql'], writer['postSql'], writer['username']
        )

    @property
    def job_json_file_name(self):
        return u'%s.json' % self.jd.datax_job_instance_id

    @property
    def job_json_file(self):
        return config.datax_job_dir + '/' + self.job_json_file_name

    @staticmethod
    def create_file(file, content):
        # 创建文件
        with open(file, 'w') as f:
            f.write(content)

    @property
    def cmd(self):
        return 'python %s/bin/datax.py %s > %s/%s.log' % \
              (config.datax_dir, self.job_json_file, config.datax_log_dir, self.job_json_file_name)

@shared_task(name='run')
def run(**data):
    # 执行任务
    dx = Datax(data)
    dx.jd.create_file(dx.job_json_file, dx.get_job_json())
    dx.jd.start_log()
    (status, output) = commands.getstatusoutput(dx.cmd)
    if status:
        logger.error("status:%s output:%s" % (status, output))
    result = 1 if status else 0
    dx.jd.record_result_log(result)


@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
def index(request):
    # 数据同步
    return render(request, 'datax_web/index.html', locals())


@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
@permission_required('batch_job.editBatchJob', raise_exception=PermissionDenied)
def add_job(request):
    # 新增任务
    return render(request, 'datax_web/add_job.html', locals())


@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
def update_job(request, id):
    # 更新任务
    return render(request, 'datax_web/update_job.html', locals())


@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
def monitor_job(request):
    # 任务执行实例
    return render(request, 'datax_web/monitor_job.html', locals())


@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
def monitor_job_detail(request, id):
    # 任务执行详情
    return render(request, 'datax_web/monitor_job_detail.html', locals())


@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
def get_database(request):
    # 获取 数据库信息
    def _data_processing(dt):
        # 清除数据里的密码，并对数据格式化
        del dt['passwd']
        return dtsf.get_row_by_dict_to_user(dt)
    response = HttpResponse()
    dtconn = dataconn.DatabaseConnection(logger)
    data = dtconn.ygol.getall(dataconn.dtbsif_sql)
    dtsf = dataconn.DataTransform()
    if dtconn.ygol.status:
        logger.error(u'获取数据库信息失败 %s' % dtconn.ygol.msg)
    response.write(json.dumps(map(_data_processing, data)))
    return response


@login_required
@verification(CheckJob)
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
@permission_required('batch_job.editBatchJob', raise_exception=PermissionDenied)
def add_job_data(request):
    # 新增或者修改任务数据
    response = HttpResponse()
    cur = Currency(request)
    data = cur.rq_post_json('data')
    jd = JobData(data)

    if jd.operation_type == config.OPERATION_TYPE[0]:
        result = jd.create()  # 新增
    else:
        result = jd.update()  # 更新
    response.write(json.dumps(result))
    return response


@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
def get_job_data(request):
    # 获取 任务列表数据
    response = HttpResponse()
    jd = JobData({})
    response.write(json.dumps(jd.get_job_data()))
    return response


@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
def get_update_job_data(request):
    # 获取 更新任务数据
    response = HttpResponse()
    cur = Currency(request)
    _id = cur.rq_post_json('_id')
    jd = JobData({})
    response.write(json.dumps(jd.get_job_data_by_id(_id)))
    return response


@login_required
@verification(CheckJob)
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
@permission_required('batch_job.editBatchJob', raise_exception=PermissionDenied)
def run_job(request):
    # 执行任务
    response = HttpResponse()
    cur = Currency(request)
    data = cur.rq_post_json('data')
    # run(**data)
    run.delay(**data)
    response.write(json.dumps(_SUCCESS))
    return response


class DataxJobInstanceSql(object):
    # datax job instance 查询sql
    _table_dji = {
        'name': {'data_type': 'str', 'val': ''},
        'description': {'data_type': 'str', 'val': ''},
        'reader_databaseinfo_host': {'data_type': 'str', 'val': ''},
        'writer_table': {'data_type': 'str', 'val': ''},
        'writer_databaseinfo_host': {'data_type': 'str', 'val': ''},
        'status': {'data_type': 'str', 'val': ''},
        'result': {'data_type': 'str', 'val': ''},
        'trigger_mode': {'data_type': 'str', 'val': ''},
    }

    _order_by = [{'table': 'dji', 'field': 'start_time', 'rule': 'DESC'}]

    def __init__(self, request):
        self.cur = Currency(request)
        self.rq_get = self.cur.rq_get

        self._offset = int(self.rq_get('offset'))
        self._limit = int(self.rq_get('limit'))
        self._SQL = config.select_datax_job_instance_sql
        self._TOTAL_SQL = config.count_datax_job_instance_sql
        self._set_table(self._table_dji)

    def _set_table(self, table):
        for field, attr in table.items():
            val = self.rq_get(field)
            attr['val'] = val
        return table

    @property
    def tables(self):
        _tables = {'dji': self._table_dji}
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


class PaginatorData(dataconn.DatabaseConnection, query_sql.Q_Data):
    # 分页访问数据
    def __init__(self, qs):
        super(PaginatorData, self).__init__(logger)
        query_sql.Q_Data.__init__(self, qs)

    @property
    def rows(self):
        return self._get_rows(self.ygol)

    @property
    def total(self):
        return self._get_total(self.ygol)


@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
def get_datax_job_instance(request):
    # 分页查询任务实例
    dsql = DataxJobInstanceSql(request)
    cvtpara = dsql.cvtpara
    tables = dsql.tables
    qs = query_sql.Q_Sql(cvtpara, **tables)
    pd = PaginatorData(qs)
    response = HttpResponse()
    response.write(json.dumps({'rows': pd.rows, 'total': pd.total}))
    return response


@login_required
@permission_required('batch_job.viewBatchJob', raise_exception=PermissionDenied)
def get_datax_job_instance_by_id(request):
    # 根据ID查询任务实例
    cur = Currency(request)
    _id = cur.rq_post('_id')
    conn = dataconn.DatabaseConnection(logger)
    dtf = dataconn.DataTransform()
    sql = config.select_datax_job_instance_by_id_sql % _id
    source_data = conn.ygol.getsingle(sql)
    response = HttpResponse()
    response.write(json.dumps(dtf.get_row_by_dict_to_user(source_data)))
    return response
