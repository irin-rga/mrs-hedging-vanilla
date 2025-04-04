from functools import wraps
from timeit import Timer
from time import time

def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print('func:%r args:[%r, %r] took: %2.4f sec' % \
          (f.__name__, args, kw, te-ts))
        return result
    return wrap

# def timer(func):
#     @wraps(func)
#     def wrapper(*args, **kwargs):
#         start_time = time.perf_counter()
#         result = func(*args, **kwargs)
#         end_time = time.perf_counter()
#         execution_time = end_time - start_time
#         print(f"{func.__name__} executed in {execution_time:.4f} seconds")
#         return result
#     return wrapper

# def timeit_decorator(func):
def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):        
        ts = time()
        result = func(*args, **kwargs)
        te = time()
        print(f"Function {func.__name__} took {te-ts: 2.4f} seconds")        
        return result
    return wrapper


def orig_timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        runs = 3
        # runs = 1
        timer = Timer(lambda: func(*args, **kwargs))
        time_taken = min(timer.repeat(repeat=runs, number=1))
        print(f"Function {func.__name__} took {time_taken:.4f} seconds")
        return func(*args, **kwargs)
    return wrapper