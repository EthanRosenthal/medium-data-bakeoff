import json
from pathlib import Path

import duckdb
import kaggle
from loguru import logger
from tqdm import tqdm


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


def construct_dataset(root: Path, kaggle_dataset_name: str) -> None:
    # Download the dataset from Kaggle
    logger.info("Downloading {!r} dataset from kaggle.", kaggle_dataset_name)
    download_path = root / "csv"
    download_path.mkdir(parents=True, exist_ok=True)
    kaggle.api.dataset_download_files(kaggle_dataset_name, download_path, unzip=True)

    logger.info("Converting dataset from CSV to parquet.")
    # Convert the dataset to parquet
    parquet_path = root / "parquet"
    parquet_path.mkdir(parents=True, exist_ok=True)
    csvs = list(download_path.iterdir())
    for csv in tqdm(csvs):
        outfile = parquet_path / csv.with_suffix(".parquet").name
        csv_to_parquet(csv.as_posix(), outfile.as_posix())
