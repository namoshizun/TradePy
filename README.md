
# TradePy

<img src="./docs/source/_static/logo.png" width="150" />

TradePy是一个面向证券交易的量化策略开发 + 实盘交易框架，实现了数据下载、策略回测、寻参优化以及实盘交易等量化交易全链路功能。🚧 **当前正在施工 V2 版本** 🚧，新版本是自底向上的整体重写，在不丢失原功能的基础上:

- 全面替换Pandas为[Polars](https://pola.rs/), 大幅提升性能与优化内存占用
- 实现Polars友好的内置指标计算, 相比TA-Lib性能提升约1-5倍
- 使用[Tushare](https://tushare.pro/)获取市场数据。近年来由于东财升级了反爬措施，已不适合使用Akshare作为主要获取手段。
- 实现更现代化的开发工具链，优化整体架构, 优化API设计
- 实现看板前端

**在线文档 (v1)**: [https://tradepy.lu-d.com](https://tradepy.lu-d.com)
