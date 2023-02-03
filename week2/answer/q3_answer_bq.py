from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from q3_answer_gcs import etl_web_to_gcs_parent

@task(retries=3)
def extract_from_gcs(year: int, month: int, color: str) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("prefect-de-zoomcamp-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"data/")
    return Path(f"data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"rows: {len(df)}")
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("sa-prefect-de-zoomcamp")

    df.to_gbq(
        destination_table="trips_data_all.yellow",
        project_id="dtc-de-zoomcamp-2023-376219",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(year, month, color):
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(year, month, color)
    df = transform(path)
    write_bq(df)
    return len(df)

@flow(log_prints=True)
def etl_parent_flow(months, year, color):
    total_rows = 0
    for month in months:
        x = etl_gcs_to_bq(year, month, color)
        total_rows += x
    print("total processed rows", total_rows)

if __name__ == "__main__":
    color = "yellow"
    months = [1, 2, 3]
    year = 2019
    etl_parent_flow(months, year, color)

    # The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.
    # Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color.
    # Make any other necessary changes to the code for it to function as required.
    # Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

