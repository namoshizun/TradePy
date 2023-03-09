from decorator import decorator


@decorator
def requirement(
    indicator_fun,
    notna=False,
    *args, **kw
):
    args[0].indicator_requirements[indicator_fun.__name__]["notna"] = notna
    return indicator_fun(*args, **kw)
