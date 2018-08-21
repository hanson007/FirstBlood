#!/usr/bin/python env
# -*- coding: UTF-8 -*-
import sys
import os
reload(sys)
sys.setdefaultencoding("utf-8")

_parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_FirstBlood_dir = os.path.dirname(_parentdir)
sys.path.append(_parentdir)
sys.path.append(_FirstBlood_dir)

_log_file_dir = _FirstBlood_dir + '/log/'
img_dir = _FirstBlood_dir + '/static/img/'

# datax job 路径
datax_dir = _FirstBlood_dir + '/datax'
datax_job_dir = '/tmp'
datax_log_dir = datax_dir + '/web_log'

# 数据申请定时任务执行日志
scheduled_tasks_log_file = _log_file_dir + u'scheduled_tasks.log'
# 日志主键
primary_key = 'task_instance_id'
# 定时任务实例状态 1:开始执行 2:正在执行 3:执行完成
status = [1, 2, 3]

# 响应类型
RESPONSE_TYPE = dict(small=1, large=2, html=3)
# 操作类型
OPERATION_TYPE = ['add', 'mod']
# 触发模式
TRIGGER_MODE = [1, 2]  # 1 自动  2 手动
# 数据库类型
DATABASE_TYPE = 'mysql'

# 以名称查询任务
query_datax_job_by_name_sql = "SELECT * FROM FirstBlood.datax_job dj WHERE dj.`name` = '%s';"
# 以名称和ID查询任务
query_datax_job_sql2 = "SELECT * FROM FirstBlood.datax_job dj WHERE dj.`name` = '%s' and dj.id!=%s;"
# 以ID查询任务
query_datax_job_by_id_sql = """
SELECT
	dj.*,
	( SELECT GROUP_CONCAT( djwc.`name` SEPARATOR '\n' ) FROM FirstBlood.datax_job_writer_column djwc WHERE djwc.datax_job_id = dj.id ) writer_column_id 
FROM
	FirstBlood.datax_job dj 
WHERE
	dj.id = %s;
"""
# 以ID查询数据同步任务需要写入的列
query_datax_job_writer_column_by_id_sql = """
SELECT
  * 
FROM
	FirstBlood.datax_job_writer_column
WHERE
	datax_job_id = %s
order by id;
"""

# 查询所有任务
query_datax_job_sql = """
SELECT
	dj.id,
	dj.`name`,
	dj.description,
	dj.querySql,
	concat(rdbi.description,'  ', rdbi.`host`) reader_databaseinfo_id,
	dj.writer_table,
	concat(wdbi.description,'  ', wdbi.`host`) writer_databaseinfo_id,
	dj.create_time,
	dj.modify_time
FROM
	FirstBlood.datax_job dj
LEFT JOIN FirstBlood.databaseinfo rdbi on dj.reader_databaseinfo_id=rdbi.id
LEFT JOIN FirstBlood.databaseinfo wdbi on dj.writer_databaseinfo_id=wdbi.id
"""

insert_datax_job_sql = """
INSERT INTO FirstBlood.datax_job (
    `name`,
    `description`,
    `querySql`,
    `reader_databaseinfo_id`,
    `writer_table`,
    `writer_databaseinfo_id`,
    `writer_preSql`,
    `writer_postSql`
) VALUES
  ('%s','%s','%s',%s,'%s',%s,'%s','%s')
"""

insert_datax_job_writer_column_sql = """
INSERT INTO FirstBlood.datax_job_writer_column (`name`, `datax_job_id`) VALUES
"""

update_datax_job_by_id_sql = """
update FirstBlood.datax_job set
    `name` = '%s',
    `description` = '%s',
    `querySql` = '%s',
    `reader_databaseinfo_id` = %s,
    `writer_table` = '%s',
    `writer_databaseinfo_id` = %s,
    `writer_preSql` = '%s',
    `writer_postSql` = '%s'
where
   id = %s
"""

delete_datax_job_writer_column_by_id_sql = """
delete from FirstBlood.datax_job_writer_column where datax_job_id =%s;
"""

insert_datax_job_instance_sql = """
insert into FirstBlood.datax_job_instance (
    `instance_id`,
    `name`,
    `description`,
    `querySql`,
    `reader_databaseinfo_host`,
    `reader_databaseinfo_description`,
    `writer_table`,
    `writer_databaseinfo_host`,
    `writer_databaseinfo_description`,
    `trigger_mode`,
    `writer_preSql`,
    `writer_postSql`
)
values (%s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', '%s');
"""


update_datax_job_instance_by_instance_id_sql = """
update FirstBlood.datax_job_instance set `status`=%s, `result`=%s, `end_time`='%s' where instance_id=%s;
"""

select_datax_job_instance_sql = """
select
  dji.id,
  dji.instance_id,
  dji.`name`,
  dji.description,
  dji.querySql,
  concat(dji.reader_databaseinfo_description,' ',dji.reader_databaseinfo_host) 'reader_databaseinfo_host',
  dji.writer_table,
  concat(dji.writer_databaseinfo_description,' ',dji.writer_databaseinfo_host) 'writer_databaseinfo_host',
  dji.writer_preSql,
  dji.writer_postSql,
  dji.trigger_mode,
  dji.`status`,
  dji.result,
  dji.start_time,
  dji.end_time
FROM
	FirstBlood.datax_job_instance dji
"""

count_datax_job_instance_sql = """
SELECT
	count(1) count
FROM
	FirstBlood.datax_job_instance dji
"""

select_datax_job_instance_by_id_sql = select_datax_job_instance_sql + "\n where dji.id = %s"


datax_job_template = """
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "mysqlreader",
                    "parameter": {
                        "connection": [
                            {
                                "jdbcUrl": ["%s"],
                                "querySql": ["%s"],
                            }
                        ],
                        "password": "%s",
                        "username": "%s",
                        "where": ""
                    }
                },
                "writer": {
                    "name": "mysqlwriter",
                    "parameter": {
                        "column": %s,
                        "connection": [
                            {
                                "jdbcUrl": "%s",
                                "table": ["%s"]
                            }
                        ],
                        "password": "%s",
                        "preSql": [%s],
                        "postSql": [%s],
                        "session": [],
                        "username": "%s",
                        "writeMode": "insert"
                    }
                }
            }
        ],
        "setting": {
            "speed": {
                      "record": "1000"
            }
        }
    }
}
"""