from pathlib import Path
from typing import Literal

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def fetch(service: str, dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""

    df = pd.read_csv(dataset_url,  compression='gzip', engine='pyarrow', low_memory=True)
    if service == 'fhv':
        df.PULocationID = df.PUlocationID.astype("float")
        df.DOLocationID = df.DOlocationID.astype("float")
    else:
        df.PULocationID = df.PULocationID.astype("float")
        df.DOLocationID = df.DOLocationID.astype("float")
    return df

def make_path(service: str, dataset_file: str, format: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    Path(f"./data/{service}_tripdata/").mkdir(parents=True, exist_ok=True)
    if format=='csv':
        path = Path(f"./data/{service}_tripdata/{dataset_file}.csv.gz")
    elif format=='parquet':
        path = Path(f"./data/{service}_tripdata/{dataset_file}.parquet")
    return path


@task()
def write_local(df: pd.DataFrame, path: Path) -> None:
    """Write DataFrame out locally as parquet file"""
    if format=='csv':
        df.to_csv(path, compression="gzip")
    elif format=='parquet':
        df.to_parquet(path, compression="gzip")


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("dez-ny-taxi")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(service: str, month: int, year: int, format: str = 'csv', pull_local = True, push_to_gcs = True) -> None:
    """The main ETL function"""
    dataset_file = f"{service}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{dataset_file}.csv.gz"

    df = fetch(service, dataset_url)
    path = make_path(service, dataset_file , format)
    if pull_local is True:
        write_local(df, path)
    if push_to_gcs is True:
        write_gcs(path)


if __name__ == "__main__":
    for month in range(1,13):
        etl_web_to_gcs(service='green', month=month, year=2019)
        etl_web_to_gcs(service='green', month=month, year=2020)
        etl_web_to_gcs(service='yellow', month=month, year=2019)
        etl_web_to_gcs(service='yellow', month=month, year=2020)
