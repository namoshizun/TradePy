import inspect, sys


def __new_getfile(object, _old_getfile=inspect.getfile):
    """
    See: https://stackoverflow.com/questions/51566497/getting-the-source-of-an-object-defined-in-a-jupyter-notebook

    `inspect.getsource` does not work out-of-box in the Jupyter notebook. This patch
    makes it work.
    """

    if not inspect.isclass(object):
        return _old_getfile(object)

    # Lookup by parent module (as in current inspect)
    if hasattr(object, "__module__"):
        object_ = sys.modules.get(object.__module__)
        if hasattr(object_, "__file__"):
            return object_.__file__

    # If parent module is __main__, lookup by methods (NEW)
    for name, member in inspect.getmembers(object):
        if (
            inspect.isfunction(member)
            and object.__qualname__ + "." + member.__name__ == member.__qualname__
        ):
            return inspect.getfile(member)
    else:
        raise TypeError("Source for {!r} not found".format(object))


def inject_hacks():
    inspect.getfile = __new_getfile
