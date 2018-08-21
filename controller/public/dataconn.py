#!/usr/bin/python env
# -*- coding: UTF-8 -*-
# Description:                    
# Author:           黄小雪
# Date:             2018年01月04日
# Company:          东方银谷
import re
import datetime
import decimal
from django.conf import settings
from mysql_helper import BusinessMysql
from sqlserver_helper import BusinessSqlserver


# 业绩平台数据库
_database = settings.DATABASES['default']
# 数据库用户名密码SQL
dtbsif_sql = 'select * from FirstBlood.databaseinfo '


class DatabaseConnection(object):
    # 数据库连接
    def __init__(self, logger):
        self._logger = logger
        self.ygol = BusinessMysql(_database['HOST'], _database['USER'],
                                  _database['PASSWORD'], _database['NAME'])

    def get_dtbs_conn(self, name):
        # 获取数据库连接
        datainfo = self.get_datainfo(name)
        businessType = {'mysql': BusinessMysql, 'sqlserver': BusinessSqlserver}
        business = businessType[datainfo['type']]
        return business(datainfo['host'], datainfo['user'], datainfo['passwd'], datainfo['db'])

    def get_dtbs_conn_by_id(self, _id):
        """
        根据数据库信息表的id获取数据库连接
        :param _id:  数据库表主键id
        :return: 数据库连接对象
        """
        datainfo = self.get_datainfo_by_id(_id)
        businessType = {'mysql': BusinessMysql, 'sqlserver': BusinessSqlserver}
        business = businessType[datainfo['type']]
        return business(datainfo['host'], datainfo['user'], datainfo['passwd'], datainfo['db'])

    def get_datainfo(self, name):
        # 获取数据库信息
        conditions_sql = "where `name`='%s'" % name
        data = self.ygol.getsingle(dtbsif_sql + conditions_sql)
        if self.ygol.status:
            self._logger.error(u'根据数据库信息表的名称，获取数据库信息失败. - Msg:' % self.ygol.msg)
        return data

    def get_datainfo_by_id(self, _id):
        # 根据数据库信息表的主键id，获取数据库信息
        conditions_sql = "where `id`=%s" % _id
        data = self.ygol.getsingle(dtbsif_sql + conditions_sql)
        if self.ygol.status:
            self._logger.error(u'根据数据库信息表的主键id，获取数据库信息失败. - Msg:' % self.ygol.msg)
        return data


class DataTransform(object):
    """
    数据转换
    """
    def __init__(self):
        self._ILLEGAL_CHARACTERS_RE = re.compile(r'[\000-\010]|[\013-\014]|[\016-\037]')

    def get_row_by_list(self, dt, database_type):
        special_characters_conversion = self.special_characters(database_type)

        row = []
        for val in dt:
            if isinstance(val, long):
                val = '%s' % str(val)
            if isinstance(val, str) or isinstance(val, unicode):
                if next(self._ILLEGAL_CHARACTERS_RE.finditer(val), None):
                    val = re.sub(self._ILLEGAL_CHARACTERS_RE, "", val)
            if isinstance(val, bool):
                val = 'true' if val else 'false'
            if isinstance(val, str):
                val = "'%s'" % special_characters_conversion(val)
            if isinstance(val, unicode):
                val = "'%s'" % special_characters_conversion(val)
            if isinstance(val, datetime.datetime):
                val = "'%s'" % val
            if isinstance(val, datetime.date):
                val = "'%s'" % val
            if val is None:
                val = 'null'
            row.append(val)

        return row

    def get_row_by_dict(self, dt, database_type):
        special_characters_conversion = self.special_characters(database_type)

        row = {}
        for key, val in dt.items():
            if isinstance(val, long):
                val = '%s' % str(val)
            if isinstance(val, str) or isinstance(val, unicode):
                if next(self._ILLEGAL_CHARACTERS_RE.finditer(val), None):
                    val = re.sub(self._ILLEGAL_CHARACTERS_RE, "", val)
            if isinstance(val, bool):
                val = 'true' if val else 'false'
            if isinstance(val, str):
                val = "%s" % special_characters_conversion(val)
            if isinstance(val, unicode):
                val = "%s" % special_characters_conversion(val)
            if isinstance(val, datetime.datetime):
                val = "%s" % val
            if isinstance(val, datetime.date):
                val = "%s" % val
            if val is None:
                val = 'null'
            if isinstance(val, decimal.Decimal):
                val = float(val)
            row[key] = val

        return row

    def get_row_by_dict_to_user(self, dt):
        # 返给用户的数据，人性化展示
        row = {}
        for key, val in dt.items():
            if isinstance(val, long):
                val = '%s' % str(val)
            if isinstance(val, str) or isinstance(val, unicode):
                if next(self._ILLEGAL_CHARACTERS_RE.finditer(val), None):
                    val = re.sub(self._ILLEGAL_CHARACTERS_RE, "", val)
            if isinstance(val, bool):
                val = 'true' if val else 'false'
            if isinstance(val, datetime.datetime):
                val = "%s" % val
            if isinstance(val, datetime.date):
                val = "%s" % val
            if val is None:
                val = 'null'
            if isinstance(val, decimal.Decimal):
                val = float(val)
            row[key] = val

        return row

    def get_row_by_list_to_excel(self, dt):
        # 列表数据，用于生成excel文件
        row = []
        for val in dt:
            if isinstance(val, long):
                val = str(val)
            if isinstance(val, str) or isinstance(val, unicode):
                if next(self._ILLEGAL_CHARACTERS_RE.finditer(val), None):
                    val = re.sub(self._ILLEGAL_CHARACTERS_RE, "", val)
            row.append(val)

        return row

    @staticmethod
    def special_characters_mysql(string):
        double_slash = re.compile(r'\\')
        single_quotes = re.compile(r'\'')
        double_quotation_marks = re.compile(r'\"')

        string = re.sub(double_slash, "\\\\", string)
        string = re.sub(single_quotes, "\\'", string)
        string = re.sub(double_quotation_marks, "\\\"", string)
        return string

    @staticmethod
    def special_characters_sqlserver(string):
        string = string.replace("'", "''")
        string = string.replace('"', '""')
        return string

    def special_characters(self, database_type):
        # 特殊字符转义
        func = {'mysql': self.special_characters_mysql, 'sqlserver': self.special_characters_sqlserver}
        return func[database_type]
