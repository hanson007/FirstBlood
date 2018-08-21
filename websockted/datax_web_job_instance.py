#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Function: 实时获取 datax 任务实例执行详情
# @Time    : 2018/7/13 9:34
# @Author  : Hanson
# @Email   : 229396865@qq.com
# @File    : datax_web_job_instance.py
# @Software: PyCharm
# @Company : 东方银谷
import sys
import os
import time
from sys import stdin, stdout
reload(sys)
sys.setdefaultencoding("utf-8")

parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parentdir)

log_dir = parentdir + '/datax/web_log/%s.json.log'

def follow(thefile):
    thefile.seek(0,2)
    while True:
        line = thefile.readline()
        if not line:
            time.sleep(1)
            continue
        yield line


if __name__ == '__main__':
    file_id = stdin.readline().strip()
    logfile = log_dir % file_id
    print('logfile: %s!' % logfile)
    stdout.flush()  # Remember to flush

    logfile_open = open(logfile, 'r')
    cont = logfile_open.readlines()
    print ''.join(cont)
    stdout.flush()

    # For each line FOO received on STDIN, respond with "Hello FOO!".
    loglines = follow(logfile_open)
    for line in loglines:
        print line,
        stdout.flush()  # Remember to flush