/*
* 创建数据库
*/
create database FirstBlood default character set utf8 collate utf8_bin;

/* 进入数据库 */
use FirstBlood;


/*
* 数据库信息
*/
CREATE TABLE `databaseinfo` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL COMMENT '名称',
  `description` varchar(255) DEFAULT NULL COMMENT '描述',
  `host` varchar(255) DEFAULT NULL COMMENT '主机',
  `user` varchar(255) DEFAULT NULL COMMENT '用户',
  `passwd` varchar(255) DEFAULT NULL COMMENT '密码',
  `db` varchar(255) DEFAULT NULL COMMENT '数据库',
  `type` varchar(255) DEFAULT NULL COMMENT '类型',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `modify_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `databaseinfo_host_c254f05e_uniq` (`host`),
  UNIQUE KEY `databaseinfo_name_a3bc8190_uniq` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8 COMMENT='数据库信息';


/*
* 数据同步任务
*/
drop table if exists `datax_job`;
CREATE TABLE `datax_job` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL COMMENT '名称',
  `description` varchar(255) DEFAULT NULL COMMENT '描述',
  `querySql` longtext COLLATE utf8_bin NOT NULL COMMENT '查询SQL语句',
  `reader_databaseinfo_id` int(11) NOT NULL COMMENT '读取数据库',
  `writer_table` varchar(255) DEFAULT NULL COMMENT '写入表名',
  `writer_databaseinfo_id` int(11) NOT NULL COMMENT '写入数据库',
  `writer_preSql` longtext COLLATE utf8_bin NOT NULL COMMENT '写入数据前执行的SQL语句',
  `writer_postSql` longtext COLLATE utf8_bin NOT NULL COMMENT '写入数据后执行的SQL语句',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `modify_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `datax_job_name_uniq` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8 COMMENT='datax数据同步任务';


/*
* 写入表的列信息
*/
drop table if exists `datax_job_writer_column`;
CREATE TABLE `datax_job_writer_column` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL COMMENT '列名',
  `datax_job_id` int(11) NOT NULL COMMENT '数据同步任务ID',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `modify_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8 COMMENT='写入表的列信息';


/*
* 数据同步任务实例
*/
drop table if exists `datax_job_instance`;
CREATE TABLE `datax_job_instance` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `instance_id` bigint(20) NOT NULL COMMENT '任务实例ID',
  `name` varchar(255) DEFAULT NULL COMMENT '名称',
  `description` varchar(255) DEFAULT NULL COMMENT '描述',
  `querySql` longtext COLLATE utf8_bin NOT NULL COMMENT '查询SQL语句',
  `reader_databaseinfo_host` varchar(255) NOT NULL COMMENT '读取数据库IP',
  `reader_databaseinfo_description` varchar(255) NOT NULL COMMENT '读取数据库描述',
  `writer_table` varchar(255) DEFAULT NULL COMMENT '写入表名',
  `writer_databaseinfo_host` varchar(255) NOT NULL COMMENT '写入数据库IP',
  `writer_databaseinfo_description` varchar(255) NOT NULL COMMENT '写入数据库描述',
  `writer_preSql` longtext COLLATE utf8_bin NOT NULL COMMENT '写入数据前执行的SQL语句',
  `writer_postSql` longtext COLLATE utf8_bin NOT NULL COMMENT '写入数据后执行的SQL语句',
  `trigger_mode` int(2) DEFAULT '1' COMMENT '触发模式 1 自动 2 手动（默认自动）',
  `status` int(2) DEFAULT '0' COMMENT '状态 0 正在执行 1 执行完成',
  `result` int(2) DEFAULT '2' COMMENT '执行结果 0 成功 1 失败 2 未知',
  `start_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
  `end_time` datetime DEFAULT NULL COMMENT '结束时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `datax_job_instance_id_uniq` (`instance_id`)
) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8 COMMENT='datax数据同步任务实例';


/*
* 批处理作业
*/
drop table if exists `batch_job`;
CREATE TABLE `batch_job` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL COMMENT '名称',
  `description` varchar(255) DEFAULT NULL COMMENT '描述',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `modify_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `batch_job_name_uniq` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8 COMMENT='批处理作业';


/*
* 批处理作业详情
*/
drop table if exists `batch_job_details`;
CREATE TABLE `batch_job_details` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `batch_job_id` int(11) NOT NULL COMMENT '批处理作业ID',
  `subjob_id` int(11) NOT NULL COMMENT '子作业ID',
  `type` int(2) NOT NUll COMMENT '类型 1 数据同步 2 SQL脚本 3 备份。 主要用于后期扩展',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `modify_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8 COMMENT='批处理作业详情';


/*
* 批处理作业执行实例
*/
drop table if exists `batch_job_instance`;
CREATE TABLE `batch_job_instance` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `instance_id` bigint(20) NOT NULL COMMENT '实例ID',
  `name` varchar(255) DEFAULT NULL COMMENT '名称',
  `description` varchar(255) DEFAULT NULL COMMENT '描述',
  `trigger_mode` int(2) DEFAULT '1' COMMENT '触发模式 1 自动 2 手动（默认自动）',
  `status` int(2) DEFAULT '0' COMMENT '状态 0 正在执行 1 执行完成',
  `result` int(2) DEFAULT '2' COMMENT '执行结果 0 成功 1 失败 2 未知',
  `start_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
  `end_time` datetime DEFAULT NULL COMMENT '结束时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `batch_job_instance_id_uniq` (`instance_id`)
) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8 COMMENT='批处理作业执行实例';


/*
* 批处理作业执行实例详情
*/
drop table if exists `batch_job_instance_details`;
CREATE TABLE `batch_job_instance_details` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `instance_id` bigint(20) NOT NULL COMMENT '实例ID',
  `subjob_instance_id` bigint(20) NOT NULL COMMENT '子作业实例ID',
  `type` int(2) NOT NUll COMMENT '类型 1 数据同步 2 SQL脚本 3 备份。 主要用于后期扩展',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8 COMMENT='批处理作业执行实例详情';

