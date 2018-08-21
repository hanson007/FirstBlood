#!/usr/bin/python env
# -*- coding: UTF-8 -*-
# Description:                    
# Author:           黄小雪
# Date:             2017年03月29日
# Company:          东方银谷
from controller.core.public import *
import decimal


class Q_Sql(object):
    """
    # 查询sql
    table_a = {
        'delivery_id': {'data_type': 'str', 'val':''},
        'customer': {'data_type': 'str', 'val':''},
        'customer_cn': {'data_type': 'str', 'val':''},
        'employee': {'data_type': 'str', 'val':''},
        'employee_cn': {'data_type': 'str', 'val':''},
        'former_employee': {'data_type': 'str', 'val':''},
        'former_employee_cn': {'data_type': 'str', 'val':''},
        'result': {'data_type': 'str', 'val':''},
        'start_time': {'data_type': 'datetime', 'val':''},
        'end_time': {'data_type': 'datetime', 'val':''},
    }

    table_b = {
        'large_area': {'data_type': 'str', 'val': '北区'},
        'store': {'data_type': 'str', 'val': '安阳一部'},
        'emp_num': {'data_type': 'str', 'val': 'CF400721'}
    }

    table_c ={ ... ... }
    ...
    ...

    tables = {'a':table_a, 'b':table_b, 'c':table_c ... ...}

    """

    def __init__(self,  cvtpara, **tables):
        self._offset = cvtpara['offset']
        self._limit = cvtpara['limit']
        self.tables = tables

        self._SQL = cvtpara['sql']
        self._TOTAL_SQL = cvtpara['total_sql']
        self._order_by = self._set_order_by(cvtpara['order_by'])

        self._para = []
        self._condition = []
        self._condition_sql = ''

        self._data()
        self._set_condition()

    def _data(self):
        # 获取数据
        for t, table in self.tables.items():
            self._set_data(t, table)

    def _set_data(self, t, table):
        # 设置查询条件
        for field, attr in table.items():

            if attr['val']:
                if attr['data_type'] == 'str':
                    self._set_str(t, field, **attr)

                if attr['data_type'] == 'datetime':
                    self._set_datetime(t, field, **attr)

    def _set_str(self, t, field, **attr):
        val = attr['val']
        self._condition.append('%s.%s = %%s' % (t, field))
        self._para.append(val)

    def _set_datetime(self, t, field, **attr):
        val = attr['val']
        tfield = field.split('_', 1)[1]  # 获取时间字段名称
        judge = field.split('_', 1)[0]  # 判断是开始还是结束时间 'start' or 'end'
        if judge == 'start':
            self._condition.append('%s.%s >= %%s' % (t, tfield))
            self._para.append(val)
        elif judge == 'end':
            self._condition.append('%s.%s <= %%s' % (t, tfield))
            self._para.append('%s 23:59:59' % val)

    def _set_order_by(self, order_by):
        _order_by_str = ''
        _fields = []

        if order_by:
            for dt in order_by:
                _table = dt['table']
                _field = dt['field']
                _rule = dt['rule']

                _field = '%s.%s %s' % (_table, _field, _rule)
                _fields.append(_field)

            _fields_str = ','.join(_fields)
            _order_by_str = 'ORDER BY %s' % _fields_str

        return _order_by_str

    def _set_condition(self):
        if self._condition:
            and_sql = '\nand '.join(self._condition)
            self._condition_sql = 'where \n%s' % and_sql

    @property
    def para(self):
        import copy
        _para = copy.deepcopy(self._para)
        _para.append(self._offset)
        _para.append(self._limit)
        return _para

    @property
    def total_para(self):
        return self._para

    @property
    def sql(self):
        _sql = '\n'.join([self._SQL, self._condition_sql,
                      self._order_by, 'limit %s,%s'])
        return _sql

    @property
    def total_sql(self):
        _total_sql = '\n'.join([self._TOTAL_SQL, self._condition_sql])
        return _total_sql


class Q_Data(object):
    # 设置投资数据
    def __init__(self, qs):
        self._sql = qs.sql
        self._para = qs.para
        self._total_sql = qs.total_sql
        self._total_para = qs.total_para

    def _get_data(self, databases_c):
        return databases_c.getall(self._sql, self._para)

    def _data_clean(self, data):
        res = {}
        for key,val in data.items():
            res[key] = self._data_conversion(val)
        return res

    def _data_conversion(self, val):
        # 数据转换
        new_val = None
        if isinstance(val, datetime.datetime):
            new_val = val.strftime('%Y-%m-%d %H:%M:%S')

        if isinstance(val, decimal.Decimal):
            new_val = float(val)

        return new_val or val

    def _get_rows(self, databases_c):
        # rows [{key1: val1, key2: val2, ... ...}]
        rows = []
        data = self._get_data(databases_c)
        for dt in data:
            row = self._data_clean(dt)
            rows.append(row)

        return rows

    def _get_total(self, databases_c):
        total_data = databases_c.getall(self._total_sql, self._total_para)
        total = total_data[0]['count']
        return total


class Download_Sql(Q_Sql):
    """
    导出文件的sql
    查询数据时使用重写的sql和total_para去查询
    主要是去掉offset limit，导出数据时不需要分页查询
    """
    def __init__(self, cvtpara, **tables):
        super(Download_Sql, self).__init__(cvtpara, **tables)

    @property
    def sql(self):
        _sql = '\n'.join([self._SQL, self._condition_sql,
                        self._order_by])
        return _sql

    @property
    def para(self):
        return self._para