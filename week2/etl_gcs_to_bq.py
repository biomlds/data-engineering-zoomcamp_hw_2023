from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


# @task()
# def transform(path: Path) -> pd.DataFrame:
#     """Data cleaning example"""
#     df = pd.read_parquet(path)
#     print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
#     df["passenger_count"].fillna(0, inplace=True)
#     print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
#     return df

@task()
def make_df(path: Path) -> pd.DataFrame:
    """Create df from gcs"""
    df = pd.read_parquet(path)
    n_rows = df.shape[0]
    print(f"Rows count: {n_rows}")
    return (df, n_rows)


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dez_yellow_taxi.rides",
        project_id="hip-watch-375918",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(year: int, month: int, color: str) -> int:
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs(color, year, month)
    df, n_rows = make_df(path)
    write_bq(df)

    return n_rows


@flow(log_prints=True)
def main_flow(
    months: list[int] = [2, 3], year: int = 2019, color: str = "yellow"
 ):

    n_rows_list = []

    for month in months:
        n_rows = etl_gcs_to_bq(year, month, color)
        n_rows_list.append(n_rows)
  
    total_rows = sum(n_rows_list)
    print(f"Rows per chunk: {n_rows_list}")
    print(f"Total rows added: {total_rows}. Rows per chunk: {n_rows_list}")


if __name__ == "__main__":
    main_flow()
