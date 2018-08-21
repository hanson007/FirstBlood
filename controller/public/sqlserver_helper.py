#!/usr/bin/python env
# -*- coding: UTF-8 -*-
# Description:                    
# Author:           黄小雪
# Date:             2017年11月03日
# Company:          东方银谷

import pymssql

class SqlserverHelper(object):
    """
    数据访问层
    status：查询状态,0 查询正常，1 查询失败,默认为 0
    """
    def __init__(self, host, user, passwd, db):
        self.__host = host
        self.__user = user
        self.__passwd = passwd
        self.__db = db
        self.row0 = None
        self.rowcount = None
        self.msg = ''
        self.status = 0

    def __conn(self):
        try:
            conn = pymssql.connect(server=self.__host, user=self.__user,
                                   password=self.__passwd, database=self.__db,
                                   # init_command="set names utf8",
                                   charset='utf8')
        except Exception, e:
            self.msg = '%s' % e
            self.status = 1
            conn = None
        return conn

    def getall(self, sql, paramters=None):
        conn = self.__conn()
        if not conn:
            return None
        try:
            cur = conn.cursor(as_dict=True)  # 返回字典
            cur.execute(sql, paramters)
            data = cur.fetchall()
            self.rowcount = cur.rowcount
            self.row0 = [d[0] for d in cur.description]
        except Exception, e:
            self.msg = '%s' % e
            self.status = 1
            data = None
        finally:
            cur.close()
            conn.commit()
            conn.close()
        return data

    def getallmany(self, sql, paramters=None):
        conn = self.__conn()
        if not conn:
            return None
        try:
            cur = conn.cursor(as_dict=True)  # 返回字典
            cur.executemany(sql, paramters)
            data = cur.fetchall()
        except Exception, e:
            self.msg = '%s' % e
            self.status = 1
            data = None
        finally:
            cur.close()
            conn.commit()
            conn.close()
        return data

    def getsingle(self, sql, paramters=None):
        conn = self.__conn()
        if not conn:
            return None
        try:
            cur = conn.cursor(as_dict=True)  # 返回字典
            cur.execute(sql, paramters)
            data = cur.fetchone()
        except Exception, e:
            self.msg = '%s' % e
            self.status = 1
            data = None
        finally:
            cur.close()
            conn.commit()
            conn.close()
        return data

    def insertmany(self, sql, paramters=None):
        conn = self.__conn()
        if not conn:
            return None
        try:
            cur = conn.cursor(as_dict=True)  # 返回字典
            cur.executemany(sql, paramters)
        except Exception, e:
            self.msg = '%s' % e
            self.status = 1
        finally:
            cur.close()
            conn.commit()
            conn.close()
        return None

    def insert(self, sql, paramters=None):
        conn = self.__conn()
        if not conn:
            return None
        try:
            cur = conn.cursor(as_dict=True)  # 返回字典
            cur.execute(sql, paramters)
        except Exception, e:
            self.msg = '%s' % e
            self.status = 1
        finally:
            cur.close()
            conn.commit()
            conn.close()
        return None

    def getall_list(self, sql, paramters=None):
        # 返回列表形式结果
        conn = self.__conn()
        if not conn:
            return None
        try:
            cur = conn.cursor()  # 返回列表
            cur.execute(sql, paramters)
            data = cur.fetchall()
            self.rowcount = cur.rowcount
            self.row0 = [d[0] for d in cur.description]
        except Exception, e:
            self.msg = '%s' % e
            self.status = 1
            data = None
        finally:
            cur.close()
            conn.commit()
            conn.close()
        return data

    def getall_list_sqls(self, sqls, paramters=None):
        """
            执行多个sql语句，返回列表形式结果
            sqls = [sql1, sql2, ... ...]
        """

        conn = self.__conn()
        if not conn:
            return None
        try:
            cur = conn.cursor()  # 返回列表
            for sql in sqls:
                cur.execute(sql, paramters)
            data = cur.fetchall()
            self.rowcount = cur.rowcount
            self.row0 = [d[0] for d in cur.description]
        except Exception, e:
            self.msg = '%s' % e
            self.status = 1
            data = None
        finally:
            cur.close()
            conn.commit()
            conn.close()
        return data

    def delete(self, sql, paramters=None):
        conn = self.__conn()
        if not conn:
            return None
        try:
            cur = conn.cursor(as_dict=True)  # 返回字典
            cur.execute(sql, paramters)
        except Exception, e:
            self.msg = '%s' % e
            self.status = 1
        finally:
            cur.close()
            conn.commit()
            conn.close()
        return None

    def dict_generator(self, sql, paramters=None):
        """
        以生成器方式获取数据，用于数据量大的时候
        :param sql:
        :param paramters:
        :return:
        """
        conn = self.__conn()
        try:
            if conn:
                cur = conn.cursor(as_dict=True)  # 返回字典
                cur.execute(sql, paramters)
                self.rowcount = cur.rowcount
                self.row0 = [d[0] for d in cur.description]
                data = cur.fetchone()
                while data:
                    yield data
                    data = cur.fetchone()
                cur.close()
                conn.commit()
                conn.close()
        except Exception, e:
            self.msg = '%s' % e
            self.status = 1

    def tuple_generator(self, sql, paramters=None):
        """
        以生成器方式获取数据，用于数据量大的时候
        :param sql:
        :param paramters:
        :return:
        """
        conn = self.__conn()
        try:
            if conn:
                cur = conn.cursor()  # 返回字典
                cur.execute(sql, paramters)
                self.rowcount = cur.rowcount
                self.row0 = [d[0] for d in cur.description]
                data = cur.fetchone()
                while data:
                    yield data
                    data = cur.fetchone()
                cur.close()
                conn.commit()
                conn.close()
        except Exception, e:
            self.msg = '%s' % e
            self.status = 1


class BusinessSqlserver(SqlserverHelper):
    # 业务处理层
    def __init__(self, host, user, passwd, db):
        super(BusinessSqlserver, self).__init__(host, user, passwd, db)

    def search(self, sql, para=None):
        return self.getsingle(sql, para)