实盘交易
=============


TradePy的实盘交易系统由两个部分组成，适合用于中低频交易（数秒级/分钟级），完整架构见下图:

1. **策略端**: 策略端接收行情数据，并根据策略逻辑生成交易指令。
2. **交易端**: 交易端接收策略端的交易指令，并将其转发给miniQMT终端下单（暂不支持PTrade）。

.. image:: _static/trading.png
    :width: 675px


.. warning::
    
    - 由于 `Celery <https://github.com/celery/celery>`_ 以及Redis都不支持 Windows，所以策略端和Redis实例必须部署在Linux/MacOS系统。
    - miniQMT终端则只支持Windows, 所以交易端则必须部署在Windows系统。


..  admonition:: 最低系统配置

    - **策略端**: 推荐Debian12, 4G内存, 2核CPU，具体配置请根据策略指标的计算量而定。
    - **交易端**: Windows10, 4G内存, 2核CPU。


部署策略端
-------------

**注**: 以下假定您的交易系统部署在云平台上，使用官方提供的操作系统镜像，未事先安装其他软件。

安装Python
~~~~~~~~~~~~~~~~

1. 安装Miniconda:

   1.  ``wget https://repo.anaconda.com/miniconda/Miniconda3-py311_23.5.2-0-Linux-x86_64.sh``
   2.  ``bash Miniconda3-py311_23.5.2-0-Linux-x86_64.sh``
   3. 退出终端并重新登录
   4. 验证已成功安装conda，且默认环境的Python版本为3.11.4: ``python --version``

2. 安装TA-Lib: 参考此 `教程 <https://cloudstrata.io/install-ta-lib-on-ubuntu-server/>`_


安装Redis
~~~~~~~~~~~~~~~~

1. 请根据 `官方教程 <https://redis.io/docs/getting-started/installation/install-redis-on-linux/>`_ 安装Redis
2. 进入Redis终端: ``redis-cli``
3. 在终端内设置密码: ``config set requirepass TradePyRocks``, 这里示例密码为"TradePyRocks"


安装TradePy交易版
~~~~~~~~~~~~~~~~~~~~

注: 如果还没有安装git，先用 ``apt install -y git`` 安装。

.. code-block:: console

   git clone https://github.com/namoshizun/TradePy.git
   cd TradePy
   pip install ".[bot]"


初始化
~~~~~~~~~~~~~~~~~~~~

运行 ``python -m tradepy.cli.bootstrap``，在输入"运行模式"时，建议输入"paper-trading"，然后先用模拟账户进行交易策略的测试。示例如下:

.. code-block:: console

   (base) root@VM-12-17-debian:~# python -m tradepy.cli.bootstrap
   [TradePy初始化程序]
   > 请输入运行模式 (backtest=回测, paper-trading=模拟交易, live-trading=实盘交易) : paper-trading
   > 是否为交易端? (y/n): n
   > 请输入K线数据的下载目录（完整地址）: /root/database
   > 请输入Redis地址（默认localhost）: 
   > 请输入Redis端口（默认6379）: 
   > 请输入Redis密码: TradePyRocks
   > 请输入交易端服务地址: 192.168.31.88
   > 请输入交易端服务端口（默认8000）: 
    ~ 检查交易端服务地址是否可达 ...ok!
   👌 已创建配置文件: /root/.tradepy/config.yaml
   🚨 策略端的TradePy配置文件内，还需要手动填入您的交易策略的配置项


部署交易端
-----------------

安装Python环境
~~~~~~~~~~~~~~~~~~~~

下载官方版 `Python 3.11.4 <https://www.python.org/downloads/release/python-3114/>`_，安装时注意选择将Python添加到系统环境变量。


安装QMT以及XtQuant
~~~~~~~~~~~~~~~~~~~~

到官网下载 `XtQuant <http://docs.thinktrader.net/pages/633b48/>`_，解压后将xtquant文件夹移动到Python的本地库目录下，一般为: ``C:\Users\用户名\AppData\Local\Programs\Python\Python311\Lib\site-packages``。然后安装并登录QMT交易端，登录时注意选择"极简模式"



安装TradePy交易端
~~~~~~~~~~~~~~~~~~~~

注: 如果还没有安装git，请先下载Git Windows版。

.. parsed-literal::

   git clone https://github.com/namoshizun/TradePy.git
   cd TradePy
   pip install ".[broker]"


初始化
~~~~~~~~~~~~~~~~~~~~

1. 保持QMT交易端处于登录状态。

2. TODO


配置策略端TradePy
----------------------
TODO



注意事项
----------------------
TODO
