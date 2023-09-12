版本: |release|; Github地址: https://github.com/namoshizun/TradePy; Gitee镜像: `dilu3100/TradePy <https://gitee.com/dilu3100/TradePy>`_


简介
-------------------
TradePy是一个面向证券交易的量化策略开发 + 实盘交易框架，实现了如下功能:

* **数据下载**

  * 通过 `Akshare <https://github.com/akfamily/akshare/>`_ 下载A股股票、ETF和宽基指数的日K，以及后复权因子、解禁日期等数据
  * 通过 `XtQuant <http://docs.thinktrader.net/pages/4a989a>`_ 下载日内1分钟K线。
  * 数据下载为本地CSV或Pickle文件，不需要安装数据库，下载后即可离线使用


* **策略实现**: 提供声明式API以快速实现策略逻辑，内置多种常用指标，使用 `Numba <https://numba.pydata.org/>`_ 加速耗时操作.

* **策略回测**: 日K级别的交易回测，并生成回测报告。可用分钟K线做日内走势回测。

  * 可设置 每日最大开仓数量、最低开仓金额、个股最大仓位
  * 计算后复权股价，支持多种滑点设置
  * 可并行跑多轮回测，观察策略表现的统计特征.

* **寻参优化**: 基于网格搜索的参数寻优，并使用 `Dask Distributed <https://distributed.dask.org/>`_ 做并行化。未来将集成更智能的寻参算法，当前也支持使用自定义的寻参算法。

* **实盘交易**: 通过 `XtQuant <http://docs.thinktrader.net/pages/4a989a>`_ 执行实盘交易，并自行统计当日持仓和账户余额等信息，以规避QMT终端的诸多数据反馈不及时问题。

  * 每日自动更新日K数据。
  * 支持设置委托单过期时间，超时不成交且不在当前买一价，可自动撤单.
  * 支持微信推送交易行为和异常状态 （🚧施工中）

* **实盘/回测对比**: 读取实盘的交割单PDF，并与同期的回测结果进行比对，以验证回测结果的可信度（🚧施工中）。


示例
-------------------

以下即实现了一个基于10均与30均的金叉买入、死叉卖出策略，并过滤ST股。将父类改为 ``tradepy.strategy.base.LiveStrategy`` 即可直接用到实盘交易中。


.. code-block:: python

    from tradepy.strategy.factors import FactorsMixin
    from tradepy.strategy.base import BacktestStrategy
    from tradepy.decorators import tag

    class MovingAverageCrossoverStrategy(BacktestStrategy, FactorsMixin):

        @tag(outputs=["sma10_ref1", "sma30_ref1"], notna=True)
        def moving_averages_ref1(self, sma10, sma30):
            return sma10.shift(1), sma30.shift(1)

        def should_buy(self, sma10, sma30,
                       sma10_ref1, sma30_ref1,
                       close, company):
            if "ST" not in company:
                if (sma10_ref1 < sma30_ref1) and (sma10 > sma30):
                    return close, 1

        def should_sell(self, sma10, sma30, sma10_ref1, sma30_ref1):
            return (sma10_ref1 > sma30_ref1) and (sma10 < sma30)


安装
-------------------

**Linux或MacOS系统**

1. 安装Python3.10或3.11。推荐使用性能更佳的3.11
2. 安装TA-Lib

   - Linux系统: 参考此 `教程 <https://cloudstrata.io/install-ta-lib-on-ubuntu-server/>`_
   - MacOS系统: ``brew install ta-lib``

3. 安装并初始化TradePy

.. parsed-literal::

   # 获取代码
   # 注: 如果网络错误，也可以从镜像库克隆 https://gitee.com/dilu3100/TradePy.git
   git clone https://github.com/namoshizun/TradePy.git

   # 安装TradePy
   cd TradePy
   pip install .

   # 初始化TradePy, 运行模式输入"backtest"
   python -m tradepy.cli.bootstrap


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
