from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

# alternative to creating GCP blocks in the UI
# insert your own service_account_file path or service_account_info dictionary from the json file
# IMPORTANT - do not store credentials in a publicly available repository!


credentials_block = GcpCredentials(
    # service_account_info={}  # enter your credentials info or use the file method.
    service_account_file="../../hip-watch-375918-fb1062049386.json"
)
credentials_block.save("zoom-gcp-creds", overwrite=True)

bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("zoom-gcp-creds"),
    bucket="week3_fhv_tripdata",  # insert your  GCS bucket name
)

bucket_block.save("fhv-tripdata", overwrite=True)
