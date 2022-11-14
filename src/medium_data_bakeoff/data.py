import json
from pathlib import Path

import duckdb
import kaggle
from loguru import logger
from tqdm import tqdm

from medium_data_bakeoff import config


def partition_path(num_partitions: int) -> Path:
    """Get data path based on number partitions in the dataset."""
    return config.DATA_PATH / f"{num_partitions:02d}_partitions"


def csv_to_parquet(infile: str, outfile: str, sample_size=512_000) -> None:
    conn = duckdb.connect(":memory:")
    # We cannot autodetect columns due to some messiness in the data. For example,
    # station_id is mostly an integer except for some weird stations that have a long
    # UUID-esque ID. These types of stations do not appear in every file.
    columns = {
        "station_id": "VARCHAR",
        "num_bikes_available": "INTEGER",
        "num_ebikes_available": "VARCHAR",
        "num_bikes_disabled": "VARCHAR",
        "num_docks_available": "INTEGER",
        "num_docks_disabled": "VARCHAR",
        "is_installed": "VARCHAR",
        "is_renting": "VARCHAR",
        "is_returning": "VARCHAR",
        "station_status_last_reported": "INTEGER",
        "station_name": "VARCHAR",
        "lat": "VARCHAR",
        "lon": "VARCHAR",
        "region_id": "VARCHAR",
        "capacity": "VARCHAR",
        "has_kiosk": "VARCHAR",
        "station_information_last_updated": "VARCHAR",
        "missing_station_information": "BOOLEAN",
    }
    # Format columns correctly for the duckdb query.
    # Replace double-quotes with single-quotes.
    columns = json.dumps(columns, indent=2).replace('"', "'")
    query = f"""
    COPY (
        SELECT *
        FROM read_csv(
            '{infile}',
            columns={columns},
            sample_size={sample_size},
            header=True,
            auto_detect=False
        )
    )
    TO '{outfile}'
    (FORMAT 'PARQUET');
    """
    conn.execute(query)


def construct_dataset() -> None:
    # Download the dataset from Kaggle
    logger.info("Downloading {!r} dataset from kaggle.", config.KAGGLE_DATASET)
    config.CSV_PATH.mkdir(parents=True, exist_ok=True)
    kaggle.api.dataset_download_files(
        config.KAGGLE_DATASET, config.CSV_PATH, unzip=True
    )

    logger.info("Converting dataset from CSV to parquet.")
    # Convert the dataset to parquet
    csvs = list(config.CSV_PATH.iterdir())
    num_partitions = len(csvs)
    partition_path(num_partitions).mkdir(parnets=True, exist=True)
    for csv in tqdm(csvs):
        outfile = partition_path(num_partitions) / csv.with_suffix(".parquet").name
        csv_to_parquet(csv.as_posix(), outfile.as_posix())


def repartition(source_path: Path, destination_path: Path, num_partitions: int) -> None:
    """Repartition the dataset into a fewer number of files."""
    logger.info(
        f"Repartitioning {source_path.as_posix()} data into {num_partitions} partitions"
    )
    destination_path.mkdir(parents=True, exist_ok=True)
    files = list(source_path.iterdir())
    if num_partitions >= len(files):
        raise ValueError(
            f"num_partitions >= number of files ({num_partitions} >= {len(files)})"
        )
    partition_size = len(files) // num_partitions
    for partition, idx in tqdm(list(enumerate(range(0, len(files), partition_size)))):
        partition_files = [f.as_posix() for f in files[idx : idx + partition_size]]
        outfile = destination_path / f"partition_{partition:04d}.parquet"
        conn = duckdb.connect(":memory:")
        query = f"""
        COPY (
          SELECT *
          FROM read_parquet({partition_files})
        )
        TO '{outfile.as_posix()}'
        (FORMAT 'PARQUET');
        """
        conn.execute(query)


def make_partitions() -> None:
    DEFAULT_PARTITIONS = 50
    source_path = partition_path(DEFAULT_PARTITIONS)
    for num_partitions in config.PARTITIONS:
        logger.info("Creating {}-partition dataset", num_partitions)
        repartition(source_path, partition_path(num_partitions), num_partitions)
