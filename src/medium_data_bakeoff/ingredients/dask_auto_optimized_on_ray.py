import time

import dask_expr as dd
import ray
from ray.util.dask import enable_dask_on_ray


def bake(dataset: str) -> float:
    ray.init()
    with enable_dask_on_ray():
        start = time.time()
        df = dd.read_parquet(dataset)
        df.groupby("station_id")["num_bikes_available"].mean().compute()
        stop = time.time()
    ray.shutdown()
    return stop - start
