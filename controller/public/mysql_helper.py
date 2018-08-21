#!/usr/bin/python env
# -*- coding: UTF-8 -*-
# Description:
# Author:           黄小雪
# Date:             2017年07月12日
# Company:          东方银谷
import MySQLdb


class MysqlHelper(object):
    """
    数据访问层
    status：查询状态,0 查询正常，1 查询失败,默认为0
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
            conn = MySQLdb.connect(host=self.__host, user=self.__user,
                                   passwd=self.__passwd, db=self.__db,
                                   init_command="set names utf8;set net_write_timeout=3600;",
                                   charset='utf8',
                                   # cursorclass=MySQLdb.cursors.SSCursor
                                   )
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
            cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)  # 返回字典
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
            cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)  # 返回字典
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
            cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)  # 返回字典
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
            cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)  # 返回字典
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
            cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)  # 返回字典
            cur.execute(sql, paramters)
            setattr(self, 'insert_id', conn.insert_id())
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
            cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)  # 返回字典
            cur.execute(sql, paramters)
        except Exception, e:
            self.msg = '%s' % e
            self.status = 1
        finally:
            cur.close()
            conn.commit()
            conn.close()
        return None

    def update(self, sql, paramters=None):
        conn = self.__conn()
        if not conn:
            return None
        try:
            cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)  # 返回字典
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
                cur = conn.cursor(cursorclass=MySQLdb.cursors.SSDictCursor)  # 流式数据返回字典
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
        :param sql: SQL语句
        :param paramters:
        :param size: 每次提取的行数，默认1000。
        :return: 以生成器的方式返回数据
                 数据格式 row1 = [v1,v2, ...]
                         data = (row1, row2, ...)
        """
        conn = self.__conn()
        try:
            if conn:
                # SSCursor 查询结果缓存在server端
                cur = conn.cursor(cursorclass=MySQLdb.cursors.SSCursor)  # 返回元组
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

    def transaction_start(self):
        conn = self.__conn()
        if not conn:
            return None
        try:
            cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)  # 返回字典
            setattr(self, 'conn', conn)
            setattr(self, 'cur', cur)
        except Exception, e:
            self.msg = 'transaction_start - %s' % e
            self.status = 1
            conn.commit()
            conn.close()
        return None

    def transaction_execute(self, sql, paramters=None):
        # 事务 insert update delete
        if hasattr(self, 'cur') and hasattr(self, 'conn'):
            try:
                self.cur.execute(sql, paramters)
                setattr(self, 'insert_id', self.conn.insert_id())
            except Exception, e:
                self.msg = 'transaction_insert - %s' % e
                self.status = 1
            return None

    def transaction_commit_and_close(self):
        # 执行成功提交事务，失败回滚，最后关闭连接
        if hasattr(self, 'cur') and hasattr(self, 'conn'):
            self.cur.close()
            if self.status:
                self.conn.rollback()
            else:
                self.conn.commit()
            self.conn.close()
        return None


class BusinessMysql(MysqlHelper):
    # 业务处理层
    def __init__(self, host, user, passwd, db):
        super(BusinessMysql, self).__init__(host, user, passwd, db)

    def search(self, sql, para=None):
        return self.getsingle(sql, para)