from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

# alternative to creating GCP blocks in the UI
# insert your own service_account_file path or service_account_info dictionary from the json file
# IMPORTANT - do not store credentials in a publicly available repository!


credentials_block = GcpCredentials(
    # service_account_info={}  # enter your credentials info or use the file method.
    # service_account_file="../../hip-watch-375918-fb1062049386.json"
    service_account_file="/workspaces/hip-watch-375918-fb1062049386.json"
)
credentials_block.save("zoom-gcp-creds", overwrite=True)

bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("zoom-gcp-creds"),
    bucket="dez-ny-taxi",  # insert your  GCS bucket name
)

bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("zoom-gcp-creds"),
    bucket="dez-ny-taxi-w4",  # insert your  GCS bucket name
)

bucket_block.save("dez-ny-taxi", overwrite=True)
bucket_block.save("dez-ny-taxi-w4", overwrite=True)
