import time


def sleep(second: float):
    time.sleep(second)


def time_stamp(as_int=True) -> int:
    return int(time.time()) if as_int is True else time.time()


def format_ts(ts: float = None, f_time: str = "%Y-%m-%d %H:%M:%S") -> str:
    return time.strftime(f_time, time.localtime(ts))


def unformat_ts(time_str: str, pattern: str = "%Y-%m-%d %H:%M:%S", as_int=True) -> int:
    from datetime import datetime
    ts = datetime.strptime(time_str, pattern).timestamp()
    return int(ts) if as_int else ts


def get_formatted_date(day=None, f_time='%Y-%m-%d'):
    import datetime
    now = datetime.datetime.now()

    # 如果指定了 day 参数，则使用指定的日期，否则使用当前日期
    if day is not None:
        date_to_format = now.replace(day=day)
    else:
        date_to_format = now

    return date_to_format.strftime(f_time)
