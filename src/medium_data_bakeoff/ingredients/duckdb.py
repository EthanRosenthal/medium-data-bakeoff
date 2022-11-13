import time

import duckdb


def bake(dataset: str) -> float:
    conn = duckdb.connect(":memory:")
    start = time.time()
    query = f"""
    SELECT
      station_id
      , AVG(num_bikes_available)
    FROM read_parquet('{dataset}')
    GROUP BY 1
    """
    conn.execute(query)
    res = conn.fetchall()
    stop = time.time()
    return stop - start
