# source: https://github.com/boisalai/de-zoomcamp-2023/blob/main/week4.md

from pathlib import Path
from typing import Literal
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint


@task(retries=3, log_prints=True)
def fetch(dataset_url: str) -> pd.DataFrame:
    print(dataset_url)
    df = pd.read_csv(dataset_url, compression="gzip")
    return df


@task(log_prints=True)
def clean(color: str, df: pd.DataFrame) -> pd.DataFrame:
    if color == "yellow":
        """Fix dtype issues"""
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    if color == "green":
        """Fix dtype issues"""
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
        df["trip_type"] = df["trip_type"].astype("Int64")

    if color == "yellow" or color == "green":
        df["VendorID"] = df["VendorID"].astype("Int64")
        df["RatecodeID"] = df["RatecodeID"].astype("Int64")
        df["PULocationID"] = df["PULocationID"].astype("Int64")
        df["DOLocationID"] = df["DOLocationID"].astype("Int64")
        df["passenger_count"] = df["passenger_count"].astype("Int64")
        df["payment_type"] = df["payment_type"].astype("Int64")

    if color == "fhv":
        """Rename columns"""
        df.rename(
            {"dropoff_datetime": "dropOff_datetime"}, axis="columns", inplace=True
        )
        df.rename({"PULocationID": "PUlocationID"}, axis="columns", inplace=True)
        df.rename({"DOLocationID": "DOlocationID"}, axis="columns", inplace=True)

        """Fix dtype issues"""
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])

        # See https://pandas.pydata.org/docs/user_guide/integer_na.html
        df["PUlocationID"] = df["PUlocationID"].astype("Int64")
        df["DOlocationID"] = df["DOlocationID"].astype("Int64")

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(color: str, df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as csv file"""
    Path(f"./data_w4/{color}").mkdir(parents=True, exist_ok=True)

    # path = Path(f"data/fhv/{dataset_file}.csv.gz")
    # df.to_csv(path, compression="gzip")

    path = Path(f"./data_w4/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")

    return path


@task
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("dez-ny-taxi-w4")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def web_to_gcs(
    color: Literal["fhv", "green", "yellow"], year: Literal[2019, 2020]
) -> None:
    # color = "fhv"
    # color = "green"
    # color = "yellow"

    # year = 2019
    for month in range(1, 13):
        dataset_file = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

        df = fetch(dataset_url)
        df_clean = clean(color, df)
        path = write_local(color, df_clean, dataset_file)
        write_gcs(path)


if __name__ == "__main__":
    web_to_gcs()
