import time

import dask.dataframe as dd
from dask.distributed import Client, LocalCluster


def bake(dataset: str) -> float:
    with LocalCluster() as cluster:
        client = Client(cluster)
        start = time.time()
        # NOTE: We pass in the relevant columns for the calculation because Dask
        # currently does not do predicate pushdown. This is noticeably a bit of a cheat
        # and not fully in the spirit of the bakeoff given that this is somewhat
        # advanced knowledge that a user coming from pandas may not have.
        df = dd.read_parquet(dataset, columns=["station_id", "num_bikes_available"])
        df.groupby("station_id")["num_bikes_available"].mean().compute()
        stop = time.time()
        client.close()
    return stop - start
