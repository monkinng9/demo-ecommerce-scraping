import polars as pl
from datetime import datetime
from airflow.models import Variable
import json
import boto3

# --- Configuration ---
# Retrieve MinIO credentials from Airflow Variables
try:
    minio_access_key = Variable.get("minio_access_key")
    minio_secret_key = Variable.get("minio_secret_key")
except Exception as e:
    print("Error retrieving Airflow variables. Please ensure 'minio_access_key' and 'minio_secret_key' are set.")
    print(f"Error details: {e}")
    # Use dummy credentials for local testing if variables are not found
    minio_access_key = "minioadmin"
    minio_secret_key = "minioadmin"
# MinIO S3 connection details
minio_endpoint = "http://minio:9000"
# S3 paths
# Using current date to form the source path dynamically
ingestion_date = datetime.utcnow()
ingestion_date_str = ingestion_date.strftime('%Y-%m-%d')
source_bucket = 'data-pipeline-demo'
delta_path = f"s3://{source_bucket}/processed/product_delta"
base_product_name_path = f"s3://{source_bucket}/cache/ecommerce_product_comparisons_final.csv"

storage_options = {
    "AWS_ACCESS_KEY_ID": minio_access_key,
    "AWS_SECRET_ACCESS_KEY": minio_secret_key,
    "AWS_ENDPOINT_URL": minio_endpoint,  # For delta-rs with S3-compatible storage
    "AWS_ALLOW_HTTP": "true",  # Necessary if endpoint is HTTP, like MinIO's default
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",  # Often required for S3-compatible storage like MinIO
    "AWS_REGION": "us-east-1"  # Optional: Can be a dummy value if not applicable but sometimes helps
}

base_product_name_df = pl.scan_csv(base_product_name_path, storage_options=storage_options)
line_base_name_df = base_product_name_df.select("product_no", "query_product_name_from_line").collect()
watson_base_name_df = base_product_name_df.select("product_no", "base_product_name_from_watson").collect()


processed_df = pl.scan_delta(delta_path, storage_options=storage_options)
processed_df = processed_df.filter(pl.col("ingest_timestamp_utc").cast(pl.Date) == pl.lit(ingestion_date_str).str.to_date("%Y-%m-%d"))
line_processed_df = processed_df.filter(pl.col("ecommerce_name") == 'line_shopping').collect()
watson_processed_df = processed_df.filter(pl.col("ecommerce_name") == 'watson').collect()

## Get record that have same product name as base name of each ecommerce
# Assuming the product name column in processed_df (and thus in line_processed_df and watson_processed_df) is 'product_name'.
# If this assumption is incorrect, the 'on' parameter in the join operations below will need to be updated.

# Prepare base name DataFrames for joining
line_base_names_to_join = line_base_name_df.rename({"query_product_name_from_line": "product_name"})
watson_base_names_to_join = watson_base_name_df.rename({"base_product_name_from_watson": "product_name"})

# Filter Line Shopping products
# Keep only records from line_processed_df where 'product_name' exists in 'line_base_names_to_join'.
# This join will also bring 'product_no' from 'line_base_names_to_join'.
line_matching_df = (
    line_processed_df.sort("ingest_timestamp_utc", descending=True)
    .unique(subset=["product_name"], keep="first", maintain_order=False)
    .join(
        line_base_names_to_join,  # Use unique base product_names, includes product_no
        on="product_name",
        how="inner",
    )
)

# Filter Watsons products
# Keep only records from watson_processed_df where 'product_name' exists in 'watson_base_names_to_join'.
# This join will also bring 'product_no' from 'watson_base_names_to_join'.
watson_matching_df = (
    watson_processed_df.sort("ingest_timestamp_utc", descending=True)
    .unique(subset=["product_name"], keep="first", maintain_order=False)
    .join(
        watson_base_names_to_join,  # Use unique base product_names, includes product_no
        on="product_name",
        how="inner",
    )
)

# Output the shapes of the resulting DataFrames for verification
print(f"Line Shopping - Original count: {line_processed_df.height}, Matched count: {line_matching_df.height}")
print(f"Watsons - Original count: {watson_processed_df.height}, Matched count: {watson_matching_df.height}")

## join left watson_matching_df - line_matching_df with product no 
## output: product_no, product_name (from watsan), price_from_line (sale_price from line), price_from_watson (sale_price from watson), diff

# Prepare DataFrames for joining
watson_to_join = watson_matching_df.select([
    pl.col("product_no"),
    pl.col("product_name"),
    pl.col("sale_price").alias("price_from_watson")
])

line_to_join = line_matching_df.select([
    pl.col("product_no"),
    pl.col("sale_price").alias("price_from_line")
])

# Perform the left join
comparison_df = watson_to_join.join(
    line_to_join,
    on="product_no",
    how="left"
)

# Calculate the price difference
comparison_df = comparison_df.with_columns([
    (pl.col("price_from_watson") - pl.col("price_from_line")).alias("diff")
])

# Select the final columns for output
final_comparison_df = comparison_df.select([
    pl.col("product_no"),
    pl.col("product_name"), # This is product_name from Watson as per left join
    pl.col("price_from_line"),
    pl.col("price_from_watson"),
    pl.col("diff"),
    pl.col("ingest_timestamp_utc") # Added ingest_timestamp_utc
])

print("\nPrice Comparison Results:")
print(final_comparison_df.head())


import os
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive'] # Full access to manage files and folders

def authenticate_google_drive():
    """
    Authenticates with Google Drive API using a service account for headless environments.
    Returns the Google Drive API service object.

    This method is suitable for use in Docker containers and automated scripts
    where user interaction for OAuth2 flow is not possible.

    Prerequisites:
    1. A Google Cloud Platform service account with appropriate permissions for Google Drive.
    2. The service account key JSON file downloaded from GCP.
    3. The `SCOPES` variable must be defined globally in this script, e.g.,
       SCOPES = ['https://www.googleapis.com/auth/drive']
    4. The path to the service account key file must be available. This can be set
       via the `GCP_SERVICE_ACCOUNT_FILE_PATH` environment variable. If the environment
       variable is not set, it defaults to '/opt/airflow/gcp_service_account.json'.
       Ensure this file is correctly mounted or copied into your Docker environment.
    
    Args:
        None

    Returns:
        googleapiclient.discovery.Resource: The Google Drive API service object.
    
    Raises:
        FileNotFoundError: If the service account key file is not found at the determined path.
        NameError: If the global `SCOPES` variable is not defined.
        google.auth.exceptions.DefaultCredentialsError: If the service account file is invalid or lacks permissions.
    """
    # Determine the service account file path from an environment variable or use a default.
    SERVICE_ACCOUNT_FILE = os.getenv('GCP_SERVICE_ACCOUNT_FILE_PATH', '/opt/airflow/private_key.json')

    # Ensure SCOPES is defined globally. This is consistent with the original function's usage.
    if 'SCOPES' not in globals():
        raise NameError(
            "The global variable 'SCOPES' must be defined in your script, e.g., "
            "SCOPES = ['https://www.googleapis.com/auth/drive']"
        )

    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        error_message = (
            f"Service account key file not found. Path tried: '{SERVICE_ACCOUNT_FILE}'. "
            f"Ensure the file exists and is accessible. If using the default path, place it at "
            f"'/opt/airflow/gcp_service_account.json'. Alternatively, set the "
            f"'GCP_SERVICE_ACCOUNT_FILE_PATH' environment variable to its location."
        )
        raise FileNotFoundError(error_message)

    # Load credentials from the service account file.
    # Note: google.oauth2.service_account.Credentials should be imported, e.g.,
    # from google.oauth2.service_account import Credentials as ServiceAccountCredentials
    creds = ServiceAccountCredentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    
    # Build the Google Drive service object.
    service = build('drive', 'v3', credentials=creds)
    return service

def share_file_with_user(service, file_id, email_address):
    """
    Shares a file on Google Drive with a specific user.

    Args:
        service: The authenticated Google Drive API service object.
        file_id (str): The ID of the file to share.
        email_address (str): The email address of the user to share with.
    """
    try:
        permission = {
            'type': 'user',
            'role': 'reader',  # Can be 'writer' for edit access
            'emailAddress': email_address
        }
        email_message = "A new price comparison report has been automatically generated and shared with you."
        service.permissions().create(
            fileId=file_id,
            body=permission,
            sendNotificationEmail=True,  # Send an email to reduce spam classification
            emailMessage=email_message,  # Add a custom message to the email
            supportsAllDrives=True       # Best practice for compatibility
        ).execute()
        print(f"File shared with {email_address}.")
    except HttpError as error:
        print(f"An error occurred while sharing the file: {error}")

def upload_excel_to_drive(file_path, parent_folder_id):
    """
    Uploads an Excel file to a specific parent folder in Google Drive.

    Args:
        file_path (str): The local path to the Excel file.
        parent_folder_id (str): The ID of the parent folder in Google Drive.
    """
    service = authenticate_google_drive()

    # --- Pre-upload Permission Check ---
    try:
        # Check if the service account has writer permissions on the folder.
        folder = service.files().get(
            fileId=parent_folder_id, fields='id, name, capabilities'
        ).execute()
        
        can_add_children = folder.get('capabilities', {}).get('canAddChildren')

        if not can_add_children:
            print(f"Permission Denied: The service account does not have permission to add files to the folder '{folder.get('name')}' (ID: {parent_folder_id}).")
            print("Please ensure the service account email has 'Editor' access to this Google Drive folder.")
            return None # Stop the upload process
        else:
            print(f"Successfully verified write access to the folder: '{folder.get('name')}'")

    except HttpError as error:
        print(f"Error checking folder permissions: {error}")
        print(f"Could not access the folder with ID: {parent_folder_id}. Please ensure the folder exists and is shared with the service account.")
        return None # Stop the upload process

    # Get the file name from the local path
    file_name = os.path.basename(file_path)

    file_metadata = {
        'name': file_name,
        'parents': [parent_folder_id]
    }

    media = MediaFileUpload(file_path,
                            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                            resumable=True)
    try:
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id,name,webContentLink,webViewLink',
            supportsAllDrives=True  # Best practice for compatibility
        ).execute()
        print(f"File '{file.get('name')}' uploaded to Google Drive.")
        print(f"File ID: {file.get('id')}")
        if 'webViewLink' in file:
            print(f"View Link: {file.get('webViewLink')}")
        if 'webContentLink' in file:
            print(f"Download Link: {file.get('webContentLink')}")

        # Share the file with the specified user
        share_file_with_user(service, file.get('id'), 'beyondbegin41@gmail.com')

        return {
            'file_id': file.get('id'),
            'view_link': file.get('webViewLink'),
            'download_link': file.get('webContentLink')
        }
    except HttpError as error:
        print(f"An error occurred: {error}")
        return None



def write_links_to_minio(file_details, bucket_name, object_name, storage_options):
    """
    Writes Google Drive file details (ID, links) to a JSON file in MinIO.

    Args:
        file_details (dict): A dictionary containing file_id, view_link, and download_link.
        bucket_name (str): The MinIO bucket to write to.
        object_name (str): The full path/name for the object in MinIO.
        storage_options (dict): Connection details for MinIO.
    """
    if not file_details or not file_details.get('file_id'):
        print("No file details to write to MinIO.")
        return

    # Convert the dictionary to a JSON string
    json_content = json.dumps(file_details, indent=4)

    try:
        # boto3 uses slightly different keys for its client configuration
        s3_client = boto3.client(
            's3',
            endpoint_url=storage_options.get("AWS_ENDPOINT_URL"),
            aws_access_key_id=storage_options.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=storage_options.get("AWS_SECRET_ACCESS_KEY"),
            region_name=storage_options.get("AWS_REGION", "us-east-1")
        )

        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=json_content
        )
        print(f"Successfully wrote file links to MinIO: s3://{bucket_name}/{object_name}")

    except Exception as e:
        print(f"An error occurred while writing to MinIO: {e}")

# --- Upload to Google Drive ---
# Define paths and IDs for the operation.
current_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
EXCEL_FILE_PATH = f'/tmp/product_comparison_{current_timestamp}.xlsx'
GOOGLE_DRIVE_FOLDER_ID = '1EXRnLYXXxjc46nBcJ8DYKjEhv5zd-xfE'  # ID of the 'share-with-service' folder.
MINIO_LINKS_PATH = f'metadata/gdrive_links/product_comparison_{current_timestamp}.json'

try:
    # First, check if the DataFrame is empty.
    if final_comparison_df.is_empty():
        print("Comparison DataFrame is empty. Skipping file creation and upload.")
        # You might want to exit gracefully or handle this case as needed.
    else:
        print(f"Comparison DataFrame contains {final_comparison_df.height} rows. Proceeding with file creation.")
        final_comparison_df.write_excel(EXCEL_FILE_PATH)
        print(f"Successfully saved comparison data to temporary Excel file: {EXCEL_FILE_PATH}")

        # Upload the generated Excel file to the specified Google Drive folder.
        gdrive_file_details = upload_excel_to_drive(EXCEL_FILE_PATH, GOOGLE_DRIVE_FOLDER_ID)

        # If upload was successful, write the links to a JSON file in MinIO.
        if gdrive_file_details:
            write_links_to_minio(
                file_details=gdrive_file_details,
                bucket_name=source_bucket,  # Re-use the bucket name from earlier config
                object_name=MINIO_LINKS_PATH,
                storage_options=storage_options
            )

except Exception as e:
    print(f"An error occurred during the file processing and upload pipeline: {e}")
    # Depending on the desired behavior, you might want to raise the exception
    # to make the Airflow task fail explicitly.
    # raise e