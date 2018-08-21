#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Function: 批处理作业配置文件
# @Time    : 2018/7/20 15:35
# @Author  : Hanson
# @Email   : @qq.com
# @File    : config.py
# @Software: PyCharm
# @Company : 东方银谷

# 最大任务个数
maxtasksperchild = 20

query_batch_job_by_name_sql = "SELECT * FROM FirstBlood.batch_job bj WHERE bj.`name` = '%s';"

query_batch_job_sql1 = "SELECT * FROM FirstBlood.batch_job bj WHERE bj.`name` = '%s' and bj.id!=%s;"

insert_batch_job_sql = "insert into FirstBlood.`batch_job` (`name`, `description`) values ('%s', '%s');"

insert_batch_job_details_sql = "insert into FirstBlood.`batch_job_details` (`batch_job_id`, `subjob_id`, `type`) values"

query_batch_job_sql2 = """
SELECT
	bj.*,
	dp.task,
	dp.enabled,
	concat(dc.`minute`,' ',dc.`hour`,' ',dc.day_of_week,' ',dc.day_of_month, ' ',dc.month_of_year, ' (m/h/d/dM/MY)') crontab
FROM
	FirstBlood.batch_job bj
LEFT JOIN FirstBlood.djcelery_periodictask dp ON CONCAT("[",bj.id,"]")=dp.args
LEFT JOIN FirstBlood.djcelery_crontabschedule dc on dp.crontab_id=dc.id
ORDER BY bj.id
"""

query_batch_job_sql3 = """
SELECT
	bj.*,
	dp.task,
	dp.enabled,
	dp.crontab_id,
	concat(dc.`minute`,' ',dc.`hour`,' ',dc.day_of_week,' ',dc.day_of_month, ' ',dc.month_of_year, ' (m/h/d/dM/MY)') crontab
FROM
	FirstBlood.batch_job bj
LEFT JOIN FirstBlood.djcelery_periodictask dp ON CONCAT("[",bj.id,"]")=dp.args
LEFT JOIN FirstBlood.djcelery_crontabschedule dc on dp.crontab_id=dc.id
WHERE bj.id = %s
"""

query_batch_job_sub_job_by_id_sql = """
SELECT
  bjd.*,
  dj.`name`,
  dj.description
FROM
  FirstBlood.batch_job_details bjd
LEFT JOIN FirstBlood.datax_job dj on bjd.subjob_id=dj.id
WHERE
  bjd.batch_job_id = %s
"""

update_batch_job_by_id_sql = """
update FirstBlood.batch_job set
    `name` = '%s',
    `description` = '%s'
where
   id = %s
"""

delete_batch_job_details_by_id_sql = """
delete from FirstBlood.batch_job_details where batch_job_id =%s;
"""

insert_batch_job_instance_sql = """
insert into FirstBlood.batch_job_instance (
    `instance_id`,
    `name`,
    `description`,
    `trigger_mode`
)
values (%s, '%s', '%s', %s);
"""

update_batch_job_instance_by_id_sql = """
update FirstBlood.batch_job_instance set `status`=%s, `result`=%s, `end_time`='%s' where instance_id=%s;
"""


insert_batch_job_instance_details_sql = """
insert into FirstBlood.batch_job_instance_details (
    `instance_id`,
    `subjob_instance_id`,
    `type`
)
values (%s, %s, %s);
"""

select_batch_job_instance_sql = "select * from FirstBlood.batch_job_instance bji"
count_batch_job_instance_sql = "select count(1) count from FirstBlood.batch_job_instance bji"
select_batch_job_instance_by_id_sql = "select * from FirstBlood.batch_job_instance bji where bji.instance_id=%s"

# 根据ID获取子作业类型为数据同步的信息
select_sub_job_datax_instance_by_id_sql = """
select
    bjid.instance_id,
    bjid.subjob_instance_id,
    bjid.type,
    dji.`name`,
    dji.description,
    dji.trigger_mode,
    dji.`status`,
    dji.result,
    dji.start_time,
    dji.end_time
from
  FirstBlood.batch_job_instance_details bjid
LEFT JOIN FirstBlood.datax_job_instance  dji on bjid.subjob_instance_id=dji.instance_id
where
  bjid.instance_id=%s and bjid.type=1
"""