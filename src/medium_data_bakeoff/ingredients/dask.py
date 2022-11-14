import time

import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from dask.distributed import Client, LocalCluster


def bake(dataset: str) -> float:
    with LocalCluster() as cluster:
        client = Client(cluster)
        ProgressBar().register()
        start = time.time()
        df = dd.read_parquet(dataset, index=False)
        df.groupby("station_id")["num_bikes_available"].mean().compute()
        stop = time.time()
        client.close()
    return stop - start
