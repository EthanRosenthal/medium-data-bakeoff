import time

import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
from dask_sql import Context


def bake(dataset: str) -> float:
    with LocalCluster() as cluster:
        client = Client(cluster)
        context = Context()
        start = time.time()
        df = dd.read_parquet(dataset, index=False)
        context.create_table("bike_availability", df)
        res = context.sql(
            """
            SELECT
              station_id
              , AVG(num_bikes_available)
            FROM bike_availability
            GROUP BY 1
            """
        )
        res = res.compute()
        client.close()
        stop = time.time()
    return stop - start
