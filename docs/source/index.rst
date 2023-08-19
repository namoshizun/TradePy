.. tradepy documentation master file, created by
   sphinx-quickstart on Thu Aug 17 13:43:52 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

版本: |release|. Python版本 v3.10, v3.11


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

* **寻参优化**: 基于网格搜索的参数寻优，并使用 `Dask Distributed <https://distributed.dask.org/>`_ 做并行化。未来将集成更智能的寻参算法，当前也支持使用自定义的寻参算法。

* **实盘交易**: 通过 `XtQuant <http://docs.thinktrader.net/pages/4a989a>`_ 执行实盘交易（需将策略端部署在Windows环境），并自行统计当日持仓和成交等信息，以规避QMT终端的诸多数据反馈不及时问题。

* **实盘/回测对比**: 读取实盘的交割单PDF，并与同期的回测结果进行比对，以验证回测结果的可信度 （开发中）。


安装
-------------------

**Linux或MacOS系统**

安装Python3.10或3.11，然后安装TradePy

.. parsed-literal::

   # 获取代码
   git clone https://github.com/namoshizun/TradePy.git

   # 安装TradePy
   cd TradePy
   pip install .


**Windows系统**

安装Python3.10或3.11，然后根据 `说明文档 <https://github.com/namoshizun/TradePy/tree/main/win64-libs#readme>`_ 安装TA-Lib以及Graphviz。完成后，以同样方式获取TradePy代码并安装。



索引
-------------------
.. toctree::
   :maxdepth: 2

   quickstart
   strategy
   optimization
   trading
   minute-k
   configurations

