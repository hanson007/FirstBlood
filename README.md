精简版ETL数据转换工具
==========
## 功能
    1.数据同步（目前只支持MySQL）
    2.执行SQL脚本 （后期开发）
    3.定时执行
    
## 安装
#### 准备环境
    1.python 环境 2.7
    2.安装requirements.txt里的依赖包，若运行时还报缺少模块的错误，再安装缺少的模块。
    3.创建日志目录
      cd <项目路径/FirstBlood>
      mkdir log

#### 建表
    整个项目所需要的表，关于用户认证、权限控制等等使用django自带的，而项目其它功能模块使用原生SQL语句创建。
    涉及到项目功能模块增删改查，全部使用原生SQL语句。
    涉及到用户认证、权限控制等等，使用Django的orm。

    1.执行 python manage.py migrate，创建项目用户认证、权限控制所需要的表（Django自带）
    2.create_table.sql 执行建表语句，创建项目中各个模块所需要的表

#### 运行其它服务
    由于项目使用到定时任务和异步实时查询日志功能，所以需要使用到celery和websockted。
    这两个服务的启动和运行全部交给supervisord托管，所以需要配置好supervisord配置文件。
    supervisord配置文件分两个，dev为开发环境的配置文件，pro为生产环境的配置文件。
    里面的路径需要根据自己实际的环境配置。

    1.配置完成后执行以下命令，启动celery和websocketed服务
      supervisord -c 项目路径/FirstBlood/supervisord/FirstBlood_pro.conf （开发环境使用FirstBlood_dev.conf文件）

    2.根据配置文件里的日志路径查看是否报错，有报错百度、Google。
      如果错误不影响功能的使用，则忽略。

#### settings配置文件
    由于项目的settings配置文件，需要根据开发环境、生产环境来连接不同环境的数据库，所以需要在开发环境添加变量。
    settings文件里就可以通过development_environment变量，选择是连接生产数据库，还是开发环境数据库。
    1.1 修改 bash_profile 文件
        vim ~/.bash_profile
        # 程序根据不同环境变量加载测试或生产的配置文件
        development_environment=1
        export development_environment

#### 下载阿里开发数据同步工具datax
    5.1 下载datax工具，放在项目目录下
        项目路径/FirstBlood/datax

    5.2 创建日志目录
        由于项目的数据同步，底层使用的datax，而datax产生的日志文件名是固定长度，且以datax的 json配置文件名命名，
        当配置文件名超过日志文件名的固定长度时，datax将会以固定长度截取配置文件名，来命名日志文件名称。所以无法以
        datax的自生的日志来实时显示同步日志。所以需要新创建日志目录 web_log，以任务ID+13位时间戳命名日志文件名，
        将datax产生的日志导入web_log目录里。

        操作命令：
        cd 项目路径/FirstBlood/datax
        mkdir web_log

## 启动
    以上步骤执行完成后，就可以运行项目。若有错误，百度Google。

## 使用
    大部分功能操作参照博客
    databaseinfo 表，需要预先导入生产库的表信息
