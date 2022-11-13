from pathlib import Path

from loguru import logger
import matplotlib.pyplot as plt
import pandas as pd

from medium_data_bakeoff.ingredients.dask import bake as bake_dask
from medium_data_bakeoff.ingredients.dask_sql import bake as bake_dask_sql
from medium_data_bakeoff.ingredients.duckdb import bake as bake_duckdb
from medium_data_bakeoff.ingredients.polars import bake as bake_polars
from medium_data_bakeoff.ingredients.spark import bake as bake_spark
from medium_data_bakeoff.ingredients.vaex import bake as bake_vaex


def plot_results(results: pd.DataFrame, plot_filename: str) -> None:
    results = results.set_index("Library").sort_values("Time (s)", ascending=False)

    # Make bar plot
    fig, ax = plt.subplots()
    results["Time (s)"].plot.barh(ax=ax)
    ax.set_title("Benchmark")
    ax.set_ylabel("Library")
    ax.set_xlabel("Time (s)")
    # Despine
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)

    # Add multiple labels to bar plot.
    labels = [f"{m:.1f}X" for m in results["multiple"].tolist()]
    for rect, label in zip(ax.patches, labels):
        width = rect.get_width()
        ax.text(
            # <MAGIC NUMBER>
            width + 4,
            rect.get_y() + rect.get_height() / 2,
            label,
            ha="left",
            va="center",
        )
    xmin, xmax = ax.get_xlim()
    # <MAGIC NUMBER>
    # (The label gets cut off by the axis.)
    ax.set_xlim(xmin, xmax + 20)
    fig.tight_layout()

    fig.savefig(plot_filename)


def bakeoff(dataset: str, results_filename: str) -> None:

    bakeoff = {}

    recipe = [
        # Dask was working on 2022.10.2. It got downgraded to 2022.10.0 after
        # installing dask-sql, and the benchmark no longer works on my machine. I think
        # it's running out of memory.
        # ("dask", bake_dask),
        ("dask_sql", bake_dask_sql),
        ("duckdb", bake_duckdb),
        ("polars", bake_polars),
        ("spark", bake_spark),
        ("vaex", bake_vaex),
    ]

    for name, func in recipe:
        logger.info("Baking {}", name)
        bakeoff[name] = func(dataset)
        logger.info(
            "{name} took {duration:.3f} seconds to bake.",
            name=name,
            duration=bakeoff[name],
        )

    results = pd.DataFrame(bakeoff.items(), columns=["Library", "Time (s)"])
    results["multiple"] = results["Time (s)"] / results["Time (s)"].min()
    results.to_csv(results_filename, index=False)
    print(results.sort_values("Time (s)"))

    plot_results(results, (Path(results_filename).parent / "benchmark.png").as_posix())
