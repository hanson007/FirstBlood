# -*- coding: UTF-8 -*-
import datetime
import time
import json

class Currency(object):
    #  通用帮助
    def __init__(self, request):
        self.request = request

    def rq_get(self, key):
        return self.request.GET.get(key, '').strip()

    def rq_post(self, key):
        return self.request.POST.get(key, '').strip()

    def rq_get_json(self, key):
        return json.loads(self.rq_get(key))

    def rq_post_json(self, key):
        return json.loads(self.rq_post(key))

class DatetimeHelp(object):
    # 日期时间帮助
    def __init__(self):
        pass

    @property
    def now_time(self):
        return datetime.datetime.now()

    def strptime(self, value, format):
        return datetime.datetime.strptime(value, format)

    @property
    def nowtimestrf1(self):
        return self.now_time.strftime(u'%Y-%m-%d %H:%M:%S')

    @property
    def nowtimestrf2(self):
        return self.now_time.strftime(u'%Y年%m月%d日 %H点%M分%S秒')

    @property
    def nowtimestrf3(self):
        return self.now_time.strftime(u'%Y%m%d%H%M%S')

    @property
    def nowtimestrf4(self):
        return self.now_time.strftime(u'%Y%m%d')

    @property
    def nowtimestrf5(self):
        return self.now_time.strftime(u'%Y-%m-%d')

    @property
    def nowtimestrf6(self):
        return self.now_time.strftime(u'%Y年%m月%d日')

    @property
    def yesterday(self):
        yd = self.now_time - datetime.timedelta(days=1)
        return yd

    @property
    def yesterdaystrf4(self):
        return self.yesterday.strftime(u'%Y%m%d')

    @property
    def yesterdaystrf5(self):
        return self.yesterday.strftime(u'%Y-%m-%d')

    @property
    def yesterdaystrf6(self):
        return self.yesterday.strftime(u'%Y年%m月%d日')

    @property
    def timestamp1(self):
        """返回当前时间的13位毫秒时间戳
        :return: 13 位的毫秒时间戳  1456402864242
        """
        return self.datetime_to_timestamp(self.now_time)

    @staticmethod
    def datetime_to_timestamp(datetime_obj):
        """将本地(local) datetime 格式的时间 (含毫秒) 转为毫秒时间戳
        :param datetime_obj: {datetime}2016-02-25 20:21:04.242000
        :return: 13 位的毫秒时间戳  1456402864242
        """
        local_timestamp = long(time.mktime(datetime_obj.timetuple()) * 1000.0 + datetime_obj.microsecond / 1000.0)
        return local_timestamp


if __name__ == '__main__':
    dth = DatetimeHelp()
    print dth.timestamp1
