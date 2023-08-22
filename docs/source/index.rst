.. tradepy documentation master file, created by
   sphinx-quickstart on Thu Aug 17 13:43:52 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

版本: |release|; Github地址: https://github.com/namoshizun/TradePy


简介
-------------------
TradePy是一个面向证券交易的量化策略开发 + 实盘交易框架，实现了如下功能:

* **数据下载**

  * 通过 `Akshare <https://github.com/akfamily/akshare/>`_ 下载A股股票、ETF和宽基指数的日K，以及后复权因子、解禁日期等数据
  * 通过 `XtQuant <http://docs.thinktrader.net/pages/4a989a>`_ 下载日内1分钟K线。
  * 数据下载为本地CSV或Pickle文件，不需要安装数据库，下载后即可离线使用


* **策略实现**: 提供声明式API以快速实现策略逻辑，并内置多种常用指标

* **策略回测**: 日K级别的交易回测，并生成回测报告。如果下载了分钟级K线，则支持以日内走势判断买卖点位。

  * 可设置 每日最大开仓数量、最低开仓金额、个股最大仓位
  * 计算后复权股价，支持多种滑点设置
  * 可并行跑多轮同参数回测

* **寻参优化**: 基于网格搜索的参数寻优，并使用 `Dask Distributed <https://distributed.dask.org/>`_ 做并行化。未来将集成更智能的寻参算法，当前也支持使用自定义的寻参算法。

* **实盘交易**: 通过 `XtQuant <http://docs.thinktrader.net/pages/4a989a>`_ 执行实盘交易（需将策略端部署在Windows环境），并自行统计当日持仓和账户余额等信息，以规避QMT终端的诸多数据反馈不及时问题。

* **实盘/回测对比**: 读取实盘的交割单PDF，并与同期的回测结果进行比对，以验证回测结果的可信度 （开发中）。


安装
-------------------

**Linux或MacOS系统**

1. 安装Python3.10或3.11
2. 安装TA-Lib

   - Linux系统: 参考此 `教程 <https://cloudstrata.io/install-ta-lib-on-ubuntu-server/>`_
   - MacOS系统: ``brew install ta-lib``

3. 安装TradePy
4. 运行初始化命令 ``python -m tradepy.cli.bootstrap``

.. parsed-literal::

   # 获取代码
   git clone https://github.com/namoshizun/TradePy.git

   # 安装TradePy
   cd TradePy
   pip install .


**Windows系统**

1. 安装Python3.10或3.11
2. 安装TA-Lib: 从 `此处 <https://github.com/cgohlke/talib-build/releases/tag/v0.4.26>`_ 下载并安装对应您的Python版本的TA-Lib Wheel文件。例如: Win10 64位系统，Python 3.10版本，Intel CPU，应下载 ``TA_Lib-0.4.26-cp310-cp310-win_amd64.whl`` 。然后运行 ``pip install TA_Lib-0.4.26-cp310-cp310-win_amd64.whl``。
3. 安装和初始化TradePy，步骤如上


索引
-------------------
.. toctree::
   :maxdepth: 2

   quickstart
   strategy
   optimization
   trading
   minute-k
   multi-backtest-runs
   configurations

