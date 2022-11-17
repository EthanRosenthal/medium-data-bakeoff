import time
import modin
import modin.pandas as pd
import pathlib
from dask.distributed import Client, LocalCluster


def bake(dataset: str) -> float:
    with LocalCluster() as cluster:
        client = Client(cluster)
        modin.config.Engine.put("Dask")
        dataset = str(pathlib.Path(dataset).parent)
        start = time.time()
        df = pd.read_parquet(dataset, index=False, columns=["station_id", "num_bikes_available"])
        df.groupby("station_id")["num_bikes_available"].mean()
        stop = time.time()
        client.close()
    return stop - start
