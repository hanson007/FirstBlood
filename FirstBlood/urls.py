# -*- coding: UTF-8 -*-
"""FirstBlood URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.9/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.views.static import serve
from django.conf.urls import include, url
from django.conf import settings
from django.contrib import admin
admin.autodiscover()
from django.contrib.auth.views import (logout,login,password_change,password_change_done)
from views import *

urlpatterns = [
    url(r'^admin/', admin.site.urls),

    url(r'^index/$', index),  # 首页
    url(r'^$', index, name='index'),
    url(r'^accounts/login/$', login, {'template_name': 'registered/login.html'}, name='django.contrib.auth.views.login'),
    url(r'^accounts/logout/$', logout, name='django.contrib.auth.views.logout'),
    url(r'^password_change/$', password_change, {
        'post_change_redirect': '/password_change_done/',
        'template_name': 'registered/password_change.html'},
        name='django.contrib.auth.views.password_change'),
    url(r'^password_change_done/$', password_change_done, {
        'template_name': 'registered/password_change_done.html'},
        name='django.contrib.auth.views.password_change_done'),
    url(r'^get_username/$', get_username),  # 获取当前登陆用户名
    url(r'^check_permission/$', check_permission),  # 检测用户权限
    url(r'^static/(?P<path>.*)$', serve, {'document_root': settings.STATIC_ROOT}),

    # 数据同步
    url(r'^datax_web/', include('datax_web.urls')),
    # 批处理作业
    url(r'^batch_job/', include('batch_job.urls')),
]
