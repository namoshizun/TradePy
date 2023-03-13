import inspect
from tradepy.core.indicator import Indicator


def tag(outputs=list(), notna=False):
    from tradepy.backtesting.strategy import StrategyBase
    assert isinstance(outputs, list)

    def inner(ind_fun):
        def dec(*args, **kwargs):
            return ind_fun(*args, **kwargs)

        # Preserve indicator compute function's signature
        sig = inspect.signature(ind_fun)
        dec_params = [
            p
            for p in sig.parameters.values()
            if p.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        ]
        dec.__signature__ = sig.replace(parameters=dec_params)
        dec.__name__ = ind_fun.__name__
        dec.__doc__ = ind_fun.__doc__
        dec.__wrapped__ = ind_fun
        dec.__qualname__ = ind_fun.__qualname__
        dec.__kwdefaults__ = getattr(ind_fun, '__kwdefaults__', None)
        dec.__dict__.update(ind_fun.__dict__)

        # Reigster the indicator
        strategy_class_name, indicator_name = ind_fun.__qualname__.split('.')
        indicator = Indicator(
            name=indicator_name,
            notna=notna,
            outputs=outputs,
            predecessors=[x.name for x in dec_params[1:]]
        )
        StrategyBase.indicators_registry.register(strategy_class_name, indicator)

        # Reigster its external outputs, which are assumed to inherit the same requirements
        for out in outputs:
            assert out != indicator_name
            out_ind = Indicator(
                name=out,
                notna=notna,
                predecessors=[indicator_name]
            )
            StrategyBase.indicators_registry.register(strategy_class_name, out_ind)

        return dec
    return inner
