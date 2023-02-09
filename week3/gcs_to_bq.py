from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def local_path(year: int, month: int) -> Path:
    """Construct local path"""
    return Path(f"data/fhv_tripdata_{year}-{month}.csv.gz")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_csv(path)
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("sa-prefect-de-zoomcamp")

    df.to_gbq(
        destination_table="fhv_tripdata.tripdata",
        project_id="dtc-de-zoomcamp-2023-376219",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(year, month):
    """Main ETL flow to load data into Big Query"""
    path = local_path(year, month)
    df = transform(path)
    write_bq(df)
    return len(df)

@flow(log_prints=True)
def etl_parent_flow(months, year):
    total_rows = 0
    for month in months:
        x = etl_gcs_to_bq(year, month)
        total_rows += x
    print("total processed rows", total_rows)

if __name__ == "__main__":
    months = [1, 2, 3]
    year = 2019
    etl_parent_flow(months, year)