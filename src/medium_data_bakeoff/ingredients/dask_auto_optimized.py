import time

import dask_expr as dd
from dask.distributed import Client, LocalCluster


def bake(dataset: str) -> float:
    with LocalCluster() as cluster:
        client = Client(cluster)
        start = time.time()
        df = dd.read_parquet(dataset)
        df.groupby("station_id")["num_bikes_available"].mean().compute()
        stop = time.time()
        client.close()
    return stop - start
