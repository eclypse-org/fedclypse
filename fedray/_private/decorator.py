import ray


def remote(*args, **kwargs):
    """Defines a remote node of a federation.

    Overrides the default behavior of `ray.remote` by ensuring the value of
    max_concurrency is at least 2. This allows to avoid a node being stuck on a single
    active training/eval session. The default value of max_concurrency is set to 100.

    The other arguments are the same as `ray.remote`.
    """
    empty_kwargs = len(kwargs) == 0
    _default_max_concurrency = 100
    if "max_concurrency" not in kwargs:
        kwargs["max_concurrency"] = _default_max_concurrency
    else:
        if kwargs["max_concurrency"] is None:
            kwargs["max_concurrency"] = _default_max_concurrency
        elif kwargs["max_concurrency"] < 2:
            raise ValueError(
                "max_concurrency must be greater than 1. Set max_concurrency to None "
                f"to use the default value of {_default_max_concurrency}. If you are "
                "using a custom max_concurrency value, make sure it is at least 2."
            )
    if empty_kwargs:
        return ray.remote(**kwargs)(*args)
    else:
        return lambda fn_or_class: ray.remote(**kwargs)(fn_or_class)
