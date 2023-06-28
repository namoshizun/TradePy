import os
import sys
import time
import errno
import signal
import inspect
import functools
import traceback
from functools import wraps
from contextlib import contextmanager
from typing import TYPE_CHECKING

import tradepy
from tradepy.core.exceptions import OperationForbidden
from tradepy.core.indicator import Indicator


if TYPE_CHECKING:
    from tradepy.core.conf import ModeType


def tag(outputs=list(), notna=False):
    from tradepy.strategy.base import StrategyBase

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
        dec.__kwdefaults__ = getattr(ind_fun, "__kwdefaults__", None)
        dec.__dict__.update(ind_fun.__dict__)

        # Reigster the indicator
        strategy_class_name, indicator_name = ind_fun.__qualname__.split(".")
        indicator = Indicator(
            name=indicator_name,
            notna=notna,
            outputs=outputs,
            predecessors=[x.name for x in dec_params[1:]],
        )
        StrategyBase.indicators_registry.register(strategy_class_name, indicator)

        # Reigster its external outputs, which are assumed to inherit the same requirements
        for out in outputs:
            assert out != indicator_name
            out_ind = Indicator(name=out, notna=notna, predecessors=[indicator_name])
            StrategyBase.indicators_registry.register(strategy_class_name, out_ind)

        return dec

    return inner


def require_mode(*modes: "ModeType"):
    def inner(fun):
        def decor(*args, **kwargs):
            if tradepy.config.common.mode not in modes:
                raise OperationForbidden(
                    f"Method {fun} is only allowed in {modes} modes"
                )
            return fun(*args, **kwargs)

        return decor

    return inner


def notify_failure(title: str, channel="wechat"):
    def send_via_wechat(title, content):
        from tradepy.notification.wechat import PushPlusWechatNotifier

        notifier = PushPlusWechatNotifier()
        notifier.send(title, content)

    def decor(fun):
        @wraps(fun)
        def inner(*args, **kwargs):
            try:
                return fun(*args, **kwargs)
            except Exception as e:
                tradepy.LOG.error(f"执行{fun}时发生错误, 推送异常: {e}")
                stack_trace_message = "\n".join(
                    traceback.format_exception(*sys.exc_info())
                )
                content = str(e) + "\n\n" + stack_trace_message[:500]

                if channel == "wechat":
                    send_via_wechat(title, content)
                raise e

        return inner

    return decor


def timeout(seconds, error_message=os.strerror(errno.ETIMEDOUT)):
    def decor(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        @functools.wraps(func)
        def inner(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return inner

    return decor


@contextmanager
def timeit():
    timer = dict()
    timer["start"] = start = time.time()
    yield timer
    timer["end"] = end = time.time()
    timer["seconds"] = round(end - start, 2)
    timer["millseconds"] = round(100 * (end - start), 1)
