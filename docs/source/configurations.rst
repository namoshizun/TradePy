配置项
==========

TradePy的默认配置文件是 ``~/.tradepy/config.yaml``，基本格式如下。如果是回测模式，则只需提供 ``common`` 配置项

.. code-block:: yaml

    common:
        # 详见CommonConf字段说明

    trading:
        # 详见TradingConf字段说明

    schedules:
        # 详见SchedulesConf字段说明


.. autopydantic_model:: tradepy.core.conf.TradePyConf

.. autopydantic_model:: tradepy.core.conf.CommonConf

.. autopydantic_model:: tradepy.core.conf.TradingConf

.. autopydantic_model:: tradepy.core.conf.SchedulesConf

.. autopydantic_model:: tradepy.core.conf.BrokerConf

.. autopydantic_model:: tradepy.core.conf.XtQuantConf

.. autopydantic_model:: tradepy.core.conf.PeriodicTasksConf

.. autopydantic_model:: tradepy.core.conf.TimeoutsConf

.. autopydantic_model:: tradepy.core.conf.StrategyConf

.. autopydantic_model:: tradepy.core.conf.SlippageConf

.. autopydantic_model:: tradepy.core.conf.BacktestConf
