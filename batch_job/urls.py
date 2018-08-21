# -*- coding: UTF-8 -*-
from django.conf.urls import url
import views

urlpatterns = [
    # Examples:
    # url(r'^$', 'YinguOnline.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),

   url(r'^index/$', views.index),  # 首页
   url(r'^add_batch_job/$', views.add_batch_job),  # 新增批处理作业
   url(r'^update_batch_job/(?P<id>\d+)/$', views.update_batch_job),  # 更新批处理作业
   url(r'^batch_job_instance/$', views.batch_job_instance),  # 批处理作业执行历史
   url(r'^batch_job_instance_details/(?P<id>\d+)/$', views.batch_job_instance_details),  # 批处理作业详情执行历史
   url(r'^get_task_template/$', views.get_task_template),  # 获取任务模板
   url(r'^get_crontab/$', views.get_crontab),  # 获取crontab
   url(r'^add_crontab/$', views.add_crontab),  # 新增 crontab  定时时间
   url(r'^add_batch_job_data/$', views.add_batch_job_data),  # 提交新增或更新批处理作业数据
   # url(r'^add_job_data/$', views.add_job_data),  # 新增或修改任务数据
   url(r'^get_batch_job_data/$', views.get_batch_job_data),  # 获取批处理作业数据
   url(r'^get_batch_job_data_by_id/$', views.get_batch_job_data_by_id),  # 根据ID获取需要更新的任务数据
   url(r'^get_batch_job_sub_job_by_id/$', views.get_batch_job_sub_job_by_id),  # 根据ID获取需要更新的子作业数据
   url(r'^get_batch_job_instance/$', views.get_batch_job_instance),  # 获取批处理作业实例数据
   url(r'^get_batch_job_instance_data_by_id/$', views.get_batch_job_instance_data_by_id),  # 根据ID获取批处理作业实例数据
   url(r'^get_batch_job_sub_job_instance_data_by_id/$',
       views.get_batch_job_sub_job_instance_data_by_id),  # 根据ID获取批处理作业子作业实例数据
   url(r'^run_batch_job_task/$', views.run_batch_job_task),  # 运行批处理任务
   # url(r'^get_database/$', views.get_database),  # 获取数据库信息
]


