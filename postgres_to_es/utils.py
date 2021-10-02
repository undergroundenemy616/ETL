from functools import wraps
from time import sleep


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            t = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as ex:
                    print(ex)
                    t += start_sleep_time * 2**(factor) if t < border_sleep_time else border_sleep_time
                    sleep(t)
        return inner
    return func_wrapper


def coroutine(func):
    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        next(fn)
        return fn
    return inner
