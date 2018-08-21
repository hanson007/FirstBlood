# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.test import TestCase

# Create your tests here.

import requests
import json
import pprint
# url1 = 'http://httpbin.org/get?name=gemey&age=22'
# url2 = 'http://192.168.190.132:9000/batch_job/get_batch_job_instance/?username=admin&password=123456.com&limit=10&offset=0&name=test2&description=&status=&result=&trigger_mode='
url3 = 'http://172.24.132.144:9000/batch_job/get_batch_job_instance/?limit=10&offset=0&name=big_data&description=&status=&result=&trigger_mode='

response = requests.get(url3)
pprint.pprint(json.loads(response.text))

