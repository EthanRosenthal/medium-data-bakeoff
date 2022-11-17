from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from loguru import logger

from medium_data_bakeoff import config
from medium_data_bakeoff.data import partition_path
from medium_data_bakeoff.ingredients.dask import bake as bake_dask
from medium_data_bakeoff.ingredients.dask_sql import bake as bake_dask_sql
from medium_data_bakeoff.ingredients.modin import bake as bake_modin
from medium_data_bakeoff.ingredients.dask_on_ray import bake as bake_dask_on_ray
from medium_data_bakeoff.ingredients.modin_on_dask import bake as bake_modin_on_dask
from medium_data_bakeoff.ingredients.duckdb import bake as bake_duckdb
from medium_data_bakeoff.ingredients.polars import bake as bake_polars
from medium_data_bakeoff.ingredients.spark import bake as bake_spark
from medium_data_bakeoff.ingredients.vaex import bake as bake_vaex


def plot_results(results: pd.DataFrame, save_path: Path) -> None:
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

    fig.savefig(save_path)


def bakeoff(num_partitions: int) -> None:
    dataset = (partition_path(num_partitions) / "*.parquet").as_posix()

    results_path = config.RESULTS_PATH / f"{num_partitions:02d}_partitions"
    results_path.mkdir(parents=True, exist_ok=True)
    bakeoff = {}

    recipe = [
        ("dask* (slightly optimized)", bake_dask),
        ("dask_on_ray* (slightly optimized)", bake_dask_on_ray),
        ("modin* (slightly optimized)", bake_modin),
        ("modin_on_dask* (slightly optimized)", bake_modin_on_dask),
        ("dask_sql", bake_dask_sql),
        ("duckdb", bake_duckdb),
        ("polars", bake_polars),
        ("spark", bake_spark),
        ("vaex", bake_vaex),
    ]

    for name, func in recipe:
        logger.info("Baking {}", name)
        try:
            bakeoff[name] = func(dataset)
            logger.info(
                "{name} took {duration:.3f} seconds to bake.",
                name=name,
                duration=bakeoff[name],
            )
        except:
            logger.exception("Unknown error while baking {}", name)

    results = pd.DataFrame(bakeoff.items(), columns=["Library", "Time (s)"])
    results["multiple"] = results["Time (s)"] / results["Time (s)"].min()
    results.to_csv(results_path / f"results_{num_partitions:02d}.csv", index=False)
    print(results.sort_values("Time (s)"))

    plot_results(results, results_path / f"benchmark_{num_partitions:02d}.png")


def plot_partition_results(results: pd.DataFrame, save_path: Path) -> None:
    grouped = results.groupby(["num_partitions", "Library"])[
        ["Time (s)", "multiple"]
    ].first()
    grouped_time = grouped["Time (s)"].unstack(level=1)
    lib_order = grouped_time.mean(axis=0).sort_values().index

    fig, ax = plt.subplots()
    grouped_time[lib_order].plot.bar(ax=ax)
    ax.set_title("Partition Benchmark")
    ax.set_ylabel("Time (s)")
    ax.set_xlabel("Number of Partitions")
    # Despine
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    fig.tight_layout()

    fig.savefig(save_path)


def partition_bakeoff() -> None:
    for num_partitions in config.PARTITIONS:
        logger.info("Running bakeoff for {} partitions.", num_partitions)
        bakeoff(num_partitions)
    path = config.RESULTS_PATH / "partition_bakeoff"
    path.mkdir(parents=True, exist_ok=True)

    results = []
    for p in config.PARTITIONS:
        df = pd.read_csv(
            config.RESULTS_PATH / f"{p:02d}_partitions" / f"results_{p:02d}.csv"
        )
        df["num_partitions"] = p
        results.append(df)

    results = pd.concat(results)
    results.to_csv(path / "results.csv", index=False)

    plot_partition_results(results, path / "partition_benchmark.png")
