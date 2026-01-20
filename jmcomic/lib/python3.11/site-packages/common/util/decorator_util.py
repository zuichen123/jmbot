from .time_util import time_stamp


def timeit(topic=None, print_template='耗时{:.02f}秒', loop_times=1):
    if loop_times < 0:
        raise AssertionError('循环次数不可小于0，因为无意义')

    def with_print_template(func):
        def timeit_func(*args, **kwargs):
            x = time_stamp(as_int=False)
            obj = None
            for _ in range(loop_times):
                obj = func(*args, **kwargs)
            print((topic or func.__name__) + print_template.format(time_stamp(as_int=False) - x))
            return obj

        return timeit_func

    return with_print_template


def thread(func):
    from threading import Thread

    def thread_exec(*args, **kwargs) -> Thread:
        t = Thread(target=func, args=args, kwargs=kwargs, daemon=True)
        t.start()
        return t

    return thread_exec


def disable(_func):
    def do_nothing_func(*_args, **_kwargs):
        pass

    return do_nothing_func


def field_cache(field_name=None, sentinel=None, obj=None):
    """

    :param field_name: field name of obj
    :param sentinel: unique object used to signal cache misses
    :param obj: cache field holder
    """

    def wrapper(func):
        nonlocal field_name
        if field_name is None:
            field_name = '__cache_{}__'.format(func.__name__)

        def func_exec(*args, **kwargs):
            if obj is None:
                target = args[0]
            else:
                target = obj

            attr = getattr(target, field_name, sentinel)
            if attr is not sentinel:
                return attr

            attr = func(*args, **kwargs)
            setattr(target, field_name, attr)
            return attr

        return func_exec

    return wrapper


def trycatch(hook=lambda: None):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except KeyboardInterrupt:
                return hook()
            except BaseException:
                from common import traceback_print_exec
                traceback_print_exec()
                return hook()

        return wrapper

    return decorator
