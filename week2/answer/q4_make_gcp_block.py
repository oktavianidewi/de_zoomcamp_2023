from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

# alternative to creating GCP blocks in the UI
# insert your own service_account_file path or service_account_info dictionary from the json file
# IMPORTANT - do not store credentials in a publicly available repository!


credentials_block = GcpCredentials(
    service_account_info={
        "type": "",
        "project_id": "",
        "private_key_id": "",
        "private_key": "",
        "client_email": "",
        "client_id": "",
        "auth_uri": "",
        "token_uri": "",
        "auth_provider_x509_cert_url": "",
        "client_x509_cert_url": ""
    }  # enter your credentials info or use the file method.
)
credentials_block.save("sa-prefect-de-zoomcamp-gcs", overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("sa-prefect-de-zoomcamp-gcs"),
    bucket="",  # insert your  GCS bucket name
)

bucket_block.save("prefect-de-zoomcamp-gcs", overwrite=True)
