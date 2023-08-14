import time

import polars as pl


def bake(dataset: str) -> float:
    start = time.time()
    res = (
        pl.scan_parquet(dataset)
        .groupby("station_id")
        .agg(pl.avg("num_bikes_available"))
        .collect(streaming=True)
    )
    stop = time.time()
    return stop - start
