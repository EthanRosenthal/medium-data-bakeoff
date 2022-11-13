from pathlib import Path

import typer


app = typer.Typer(name="Medium Data Bakeoff", chain=True)

DEFAULT_ROOT = Path(__file__).parent.parent.parent / "data"


@app.command(help="Make the dataset for the bakeoff.")
def make_dataset(
    root: Path = typer.Option(DEFAULT_ROOT, help="The root path to place all files."),
    kaggle_dataset: str = typer.Option(
        "rosenthal/citi-bike-stations",
        help="The name of the Kaggle dataset to use for benchmarking.",
    ),
) -> None:
    from medium_data_bakeoff.data import construct_dataset

    construct_dataset(root, kaggle_dataset)


@app.command(help="Run the benchmark on the bakeoff dataset.")
def bakeoff(
    dataset: str = typer.Option(
        (DEFAULT_ROOT / "parquet" / "*.parquet").as_posix(),
        help="The name of the dataset. Can be a wildcard string for globbing.",
    ),
    results: str = typer.Option(
        (DEFAULT_ROOT / "results.csv"),
        help="The name of the file to store the bakeoff results.",
    ),
) -> None:
    from medium_data_bakeoff.bakeoff import bakeoff

    bakeoff(dataset, results)
