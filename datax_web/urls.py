# -*- coding: UTF-8 -*-
from django.conf.urls import url
import views

urlpatterns = [
    # Examples:
    # url(r'^$', 'YinguOnline.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),

   url(r'^index/$', views.index),  # 数据同步
   url(r'^add_job/$', views.add_job),  # 新增任务
   url(r'^monitor_job/$', views.monitor_job),  # 任务执行实例
   url(r'^monitor_job_detail/(?P<id>\d+)/$', views.monitor_job_detail),  # 任务执行详情
   url(r'^update_job/(?P<id>\d+)/$', views.update_job),  # 更新任务
   url(r'^get_database/$', views.get_database),  # 获取数据库信息
   url(r'^add_job_data/$', views.add_job_data),  # 新增或修改任务数据
   url(r'^get_job_data/$', views.get_job_data),  # 获取任务数据
   url(r'^get_update_job_data/$', views.get_update_job_data),  # 获取需要更新的任务数据
   url(r'^get_datax_job_instance/$', views.get_datax_job_instance),  # 获取任务实例数据
   url(r'^get_datax_job_instance_by_id/$', views.get_datax_job_instance_by_id),  # 根据ID获取任务实例数据
   url(r'^run_job/$', views.run_job),  # 运行 任务
   url(r'^get_database/$', views.get_database),  # 获取数据库信息
]


