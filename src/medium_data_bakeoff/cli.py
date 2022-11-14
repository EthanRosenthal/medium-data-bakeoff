import typer

app = typer.Typer(name="Medium Data Bakeoff")


@app.command(help="Make the dataset for the bakeoff.")
def make_dataset() -> None:
    from medium_data_bakeoff.data import construct_dataset

    construct_dataset()


@app.command(
    help=(
        "Repartition the dataset into a range of partitions. "
        "See medium_data_bakeoff.config.PARTITIONS for the range of partitions."
    )
)
def make_partitions() -> None:
    from medium_data_bakeoff.data import make_partitions

    make_partitions()


@app.command(help="Run the bakeoff for a single dataset.")
def bakeoff(
    num_partitions: int = typer.Option(
        50,
        help=(
            "Number of partitions in the benchmark dataset. Default corresponds to "
            "the dataset default (50)."
        ),
    )
) -> None:
    from medium_data_bakeoff.bakeoff import bakeoff

    bakeoff(num_partitions)


@app.command(help="Run the bakeoff for all partition combinations.")
def partition_bakeoff() -> None:
    from medium_data_bakeoff.bakeoff import partition_bakeoff

    partition_bakeoff()
