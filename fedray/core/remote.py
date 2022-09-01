import ray

MAX_CONCURRENCY = 1000

def remote(*args, **kwargs):
    if len(kwargs) == 0:
        return ray.remote(max_concurrency=MAX_CONCURRENCY)(*args)
    else:
        return lambda fn_or_class: ray.remote(max_concurrency=MAX_CONCURRENCY, **kwargs)(fn_or_class)
