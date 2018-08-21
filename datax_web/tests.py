# -*- coding: UTF-8 -*-
from django.test import TestCase

# Create your tests here.

#!/usr/bin/python

# Copyright 2013 Joe Walnes and the websocketd team.
# All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import time
from sys import stdin, stdout

def follow(thefile):
    thefile.seek(0,2)
    while True:
        line = thefile.readline()
        if not line:
            time.sleep(1)
            continue
        yield line

log_dir = '/opt/django/FirstBlood/datax/web_log/%s.json.log'

file_id = stdin.readline().strip()
logfile = log_dir % file_id
print('Hello fuck %s!' % logfile)
stdout.flush() # Remember to flush


# For each line FOO received on STDIN, respond with "Hello FOO!".
logfile_open = open(logfile, 'r')
loglines = follow(logfile_open)
print 'ok'
for line in loglines:
    print line,
    stdout.flush() # Remember to flush
