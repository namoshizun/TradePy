
# TradePy

![logo](./docs/source/_static/logo.png)

<p float="left">
  <img src="https://github.com/namoshizun/TradePy/actions/workflows/deploy-sphinx-doc.yml/badge.svg?branch=main&event=push" />
  <img src="https://github.com/namoshizun/TradePy/actions/workflows/run-tests.yml/badge.svg?branch=main&event=push" /> 
</p>

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


**在线文档**: https://docs.trade-py.com

**镜像仓库**: https://gitee.com/dilu3100/TradePy

**TODOs**

- [ ] 测试用例 🚧
- [ ] 微信推送交易行为
- [ ] 加载实盘交割单，与回测结果对比
- [ ] 回放回测中的交易行为
- [ ] 优化回测报告
- [ ] 调研在浏览器里使用TradePy回测功能的可行性
