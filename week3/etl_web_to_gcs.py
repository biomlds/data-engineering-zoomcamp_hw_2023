from pathlib import Path
from typing import Literal

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""

    df = pd.read_csv(dataset_url)
    df.PUlocationID = df.PUlocationID.astype("float")
    df.DOlocationID = df.DOlocationID.astype("float")

    return df


@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    Path(f"./data/fhv_tripdata/").mkdir(parents=True, exist_ok=True)
    path = Path(f"./data/fhv_tripdata/{dataset_file}.csv.gz")
    df.to_csv(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("fhv-tripdata")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(
    month: int, year: int
) -> None:
    """The main ETL function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    path = write_local(df, dataset_file)
    write_gcs(path)


if __name__ == "__main__":
    for month in range(1,12):
        etl_web_to_gcs(month=month, year=2019)
