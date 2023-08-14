import time
import modin.pandas as pd
import pathlib
import ray


def bake(dataset: str) -> float:
    dataset = str(pathlib.Path(dataset).parent)
    ray.init(runtime_env={"env_vars": {"__MODIN_AUTOIMPORT_PANDAS__": "1"}})
    start = time.time()
    df = pd.read_parquet(dataset, columns=["station_id", "num_bikes_available"])
    df.groupby("station_id")["num_bikes_available"].mean()
    stop = time.time()
    ray.shutdown()
    return stop - start
