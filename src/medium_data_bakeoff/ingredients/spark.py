import glob
import time

from pyspark.sql import SparkSession


def bake(dataset: str) -> float:
    spark = SparkSession.builder.master("local").getOrCreate()
    paths = glob.glob(dataset)
    start = time.time()
    df = spark.read.parquet(*paths)
    res = df.groupBy("station_id").agg({"num_bikes_available": "avg"}).collect()
    stop = time.time()
    spark.stop()
    return stop - start
