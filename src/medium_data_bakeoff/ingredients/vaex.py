import glob
import time

import vaex


def bake(dataset: str) -> float:
    start = time.time()
    df = vaex.open(dataset)
    res = df.groupby(
        by=df["station_id"], agg=[vaex.agg.mean("num_bikes_available")]
    ).to_pandas_df()
    stop = time.time()
    return stop - start
