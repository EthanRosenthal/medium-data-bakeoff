import time

import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from dask.distributed import Client, LocalCluster


def bake(dataset: str) -> float:
    cluster = LocalCluster()
    client = Client(cluster)
    ProgressBar().register()
    start = time.time()
    df = dd.read_parquet(dataset, index=False)
    df.groupby("station_id")["num_bikes_available"].mean().compute()
    stop = time.time()
    return stop - start
