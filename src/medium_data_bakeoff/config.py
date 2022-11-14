from pathlib import Path

# The root path of the repositor.
ROOT_PATH = Path(__file__).parent.parent.parent
# Directory in which to store all benchmarking data.
DATA_PATH = ROOT_PATH / "data"
# Directory to store the Kaggle dataset CSVs.
CSV_PATH = DATA_PATH / "csv"
# Range of number of partitions to use for partition-bakeoff.
PARTITIONS = [50, 25, 10, 5, 1]
# Directory to store bakeoff results.
RESULTS_PATH = ROOT_PATH / "results"
# Name of the Kaggle benchmark dataset.
KAGGLE_DATASET = "rosenthal/citi-bike-stations"
