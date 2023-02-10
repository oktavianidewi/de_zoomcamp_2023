from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import os

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
    df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("sa-prefect-de-zoomcamp")
    df.to_gbq(
        destination_table="fhv_tripdata.tripdata_flow",
        project_id="dtc-de-zoomcamp-2023-376219",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@flow()
def etl_gcs_to_bq(year, month, gcs_path):
    """Main ETL flow to load data into Big Query"""
    df = transform(gcs_path)
    write_bq(df)

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read fhv data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url, compression='gzip')
    return df

@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    
    # create directory if not exist
    dir = f"data/fhv"
    if not os.path.isdir(dir):
        os.makedirs(dir)

    path = Path(f"{dir}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_gcs(path: Path):
    """Upload local parquet file to GCS"""
    gcp_block = GcsBucket.load("fhv-zoomcamp-gcs")
    gcp_block.upload_from_path(from_path=path, to_path=path, timeout=120)

@flow(log_prints=True)
def etl_web_to_gcs(year, month):
    """download"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
    df = fetch(dataset_url)
    path = write_local(df, dataset_file)
    write_gcs(path)
    return path

@flow(log_prints=True)
def etl_parent_flow(months, year):
    for month in months:
        gcs_path = etl_web_to_gcs(year, month)
        etl_gcs_to_bq(year, month, gcs_path)

if __name__ == "__main__":
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    year = 2019
    etl_parent_flow(months, year)