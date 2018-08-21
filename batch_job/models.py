# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models

# Create your models here.
class BatchJobPermission(models.Model):
    """
    批处理作业权限
    """
    class Meta:
        db_table = 'batch_job_permission'
        permissions = (
            ("viewBatchJob", u"查看批处理作业"),
            ("editBatchJob", u"修改批处理祖业")
        )
