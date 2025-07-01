import polars as pl
from datetime import datetime
from airflow.models import Variable
import json
import boto3
import os
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive']  # Full access to manage files and folders

def authenticate_google_drive():
    """
    Authenticates with Google Drive API using a service account for headless environments.
    Returns the Google Drive API service object.

    This method is suitable for use in Docker containers and automated scripts
    where user interaction for OAuth2 flow is not possible.

    Raises:
        FileNotFoundError: If the service account key file is not found.
        ValueError: If the service account key file is improperly formatted.
        NameError: If the global `SCOPES` variable is not defined.
        google.auth.exceptions.DefaultCredentialsError: If the service account file is invalid or lacks permissions.
    """
    SERVICE_ACCOUNT_FILE = Variable.get("google_service_account_json_path", default_var='/opt/airflow/google_service_account.json')
    try:
        creds = ServiceAccountCredentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)
        print("Successfully authenticated with Google Drive API using service account.")
        return service
    except FileNotFoundError:
        print(f"Error: Service account key file not found at {SERVICE_ACCOUNT_FILE}.")
        raise
    except ValueError as ve:
        print(f"Error: Service account key file {SERVICE_ACCOUNT_FILE} is improperly formatted. Details: {ve}")
        raise
    except NameError as ne:
        print(f"Error: The 'SCOPES' variable is not defined. Details: {ne}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred during Google Drive authentication: {e}")
        raise

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
            'role': 'writer',  # Can be 'writer' for edit access
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
    Returns:
        dict: A dictionary containing file_id, view_link, and download_link, or None on failure.
    """
    service = authenticate_google_drive()
    if not service:
        print("Failed to authenticate with Google Drive. Aborting upload.")
        return None

    file_metadata = {
        'name': os.path.basename(file_path),
        'parents': [parent_folder_id]
    }
    media = MediaFileUpload(file_path,
                            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                            resumable=True)
    try:
        file_obj = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id,name,webContentLink,webViewLink',
            supportsAllDrives=True
        ).execute()
        print(f"File '{file_obj.get('name')}' uploaded to Google Drive.")
        print(f"File ID: {file_obj.get('id')}")
        if 'webViewLink' in file_obj:
            print(f"View Link: {file_obj.get('webViewLink')}")
        if 'webContentLink' in file_obj:
            print(f"Download Link: {file_obj.get('webContentLink')}")

        gdrive_share_email = Variable.get("google_drive_share_email", default_var='beyondbegin41@gmail.com')
        share_file_with_user(service, file_obj.get('id'), gdrive_share_email)

        return {
            'file_id': file_obj.get('id'),
            'view_link': file_obj.get('webViewLink'),
            'download_link': file_obj.get('webContentLink')
        }
    except HttpError as error:
        print(f"An error occurred during Google Drive upload: {error}")
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

    json_content = json.dumps(file_details, indent=4)

    try:
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

def create_comparison_report_and_upload(**kwargs):
    """
    Generates a product price comparison report from MinIO Delta Lake data,
    uploads it as an Excel file to Google Drive, shares it, and writes
    the GDrive links metadata back to MinIO.
    This function is intended to be called as an Airflow task.
    
    It retrieves MinIO credentials, Google Drive folder ID, service account path,
    and sharing email from Airflow Variables.
    Processes data for the current UTC day.
    """
    print("Starting comparison report generation and upload process...")
    # --- Configuration ---
    try:
        minio_access_key = Variable.get("minio_access_key")
        minio_secret_key = Variable.get("minio_secret_key")
        print("Successfully retrieved MinIO credentials from Airflow Variables.")
    except Exception as e:
        print(f"Error retrieving Airflow variables for MinIO. Using default credentials. Details: {e}")
        minio_access_key = "minioadmin"
        minio_secret_key = "minioadmin"

    minio_endpoint = Variable.get("minio_endpoint_url", default_var="http://minio:9000")
    current_processing_datetime = kwargs.get('data_interval_end') or datetime.utcnow()
    ingestion_date_str = current_processing_datetime.strftime("%Y-%m-%d")
    ingestion_date_object = current_processing_datetime.date()  # Python date object
    source_bucket = Variable.get("minio_bucket_name", default_var='data-pipeline-demo')
    delta_path = f"s3://{source_bucket}/processed/product_delta"
    base_product_name_path = f"s3://{source_bucket}/cache/ecommerce_product_comparisons_final.csv"

    storage_options = {
        "AWS_ACCESS_KEY_ID": minio_access_key,
        "AWS_SECRET_ACCESS_KEY": minio_secret_key,
        "AWS_ENDPOINT_URL": minio_endpoint,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_REGION": "us-east-1"
    }

    print(f"Reading base product names from: {base_product_name_path}")
    print(f"Reading delta data from: {delta_path} for date: {ingestion_date_str}")

    try:
        base_product_name_df = pl.scan_csv(base_product_name_path, storage_options=storage_options).collect()
        line_base_name_df = base_product_name_df.select("product_no", "query_product_name_from_line")
        watson_base_name_df = base_product_name_df.select("product_no", "base_product_name_from_watson")

        processed_df_scan = pl.scan_delta(delta_path, storage_options=storage_options)
        # Filter by ingestion date string directly on the string column before casting
        # Filter by comparing the date part of the timestamp column with the target date object
        processed_df_filtered_by_date = processed_df_scan.filter(
            pl.col("ingest_timestamp_utc").dt.date() == pl.lit(ingestion_date_object, dtype=pl.Date)
        )
        processed_df = processed_df_filtered_by_date
        
        line_processed_df = processed_df.filter(pl.col("ecommerce_name") == 'line_shopping').collect()
        watson_processed_df = processed_df.filter(pl.col("ecommerce_name") == 'watson').collect()

        print(f"Initial Line Shopping processed count: {line_processed_df.height}")
        print(f"Initial Watsons processed count: {watson_processed_df.height}")

        line_base_names_to_join = line_base_name_df.rename({"query_product_name_from_line": "product_name"})
        watson_base_names_to_join = watson_base_name_df.rename({"base_product_name_from_watson": "product_name"})

        line_matching_df = (
            line_processed_df.sort(["ingest_timestamp_utc", "sale_price"], descending=[True, False])
            .unique(subset=["product_name"], keep="first", maintain_order=False)
            .join(line_base_names_to_join, on="product_name", how="inner")
        )

        watson_matching_df = (
            watson_processed_df.sort("ingest_timestamp_utc", descending=True)
            .unique(subset=["product_name"], keep="first", maintain_order=False)
            .join(watson_base_names_to_join, on="product_name", how="inner")
        )

        print(f"Line Shopping - Matched with base names count: {line_matching_df.height}")
        print(f"Watsons - Matched with base names count: {watson_matching_df.height}")

        comparison_df = (
            watson_matching_df.select(
                pl.col("product_no"),
                pl.col("product_name"),
                pl.col("sale_price").alias("price_from_watson"),
                pl.col("ingest_timestamp_utc")
            )
            .join(
                line_matching_df.select(
                    pl.col("product_no"),
                    pl.col("sale_price").alias("price_from_line")
                ),
                on="product_no",
                how="inner"
            )
        )

        final_comparison_df = comparison_df.with_columns(
            (pl.col("price_from_line") - pl.col("price_from_watson")).alias("diff")
        ).select([
            pl.col("product_no"),
            pl.col("product_name"),
            pl.col("price_from_line"),
            pl.col("price_from_watson"),
            pl.col("diff"),
            pl.col("ingest_timestamp_utc")
        ])

        print("\nPrice Comparison Results (first 5 rows):")
        print(final_comparison_df.head())

    except Exception as e:
        print(f"Error during data processing: {e}")
        raise # Re-raise to fail the Airflow task if data processing fails

    # --- Upload to Google Drive and MinIO --- 
    current_timestamp_file = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    EXCEL_FILE_PATH = f'/tmp/product_comparison_{current_timestamp_file}.xlsx'
    # Ensure GOOGLE_DRIVE_FOLDER_ID is correctly set for your environment
    GOOGLE_DRIVE_FOLDER_ID = Variable.get("google_drive_folder_id", default_var='1EXRnLYXXxjc46nBcJ8DYKjEhv5zd-xfE') 
    MINIO_LINKS_PATH = f'metadata/gdrive_links/product_comparison_{current_timestamp_file}.json'

    try:
        if final_comparison_df.is_empty():
            print("Comparison DataFrame is empty. Skipping file creation and upload.")
        else:
            print(f"Comparison DataFrame contains {final_comparison_df.height} rows. Proceeding with file creation.")
            # Ensure /tmp directory is writable in the Airflow worker environment
            final_comparison_df.write_excel(EXCEL_FILE_PATH)
            print(f"Successfully saved comparison data to temporary Excel file: {EXCEL_FILE_PATH}")

            gdrive_file_details = upload_excel_to_drive(EXCEL_FILE_PATH, GOOGLE_DRIVE_FOLDER_ID)

            if gdrive_file_details:
                write_links_to_minio(
                    file_details=gdrive_file_details,
                    bucket_name=source_bucket,
                    object_name=MINIO_LINKS_PATH,
                    storage_options=storage_options
                )
            else:
                print("Google Drive upload failed or returned no details. Skipping MinIO link writing.")
            
            # Clean up the local Excel file
            if os.path.exists(EXCEL_FILE_PATH):
                os.remove(EXCEL_FILE_PATH)
                print(f"Cleaned up temporary file: {EXCEL_FILE_PATH}")

    except Exception as e:
        print(f"An error occurred during the file processing and upload pipeline: {e}")
        if 'EXCEL_FILE_PATH' in locals() and os.path.exists(EXCEL_FILE_PATH):
            os.remove(EXCEL_FILE_PATH) # Attempt cleanup even on error
            print(f"Cleaned up temporary file after error: {EXCEL_FILE_PATH}")
        raise # Re-raise to fail the Airflow task

    print("Comparison report generation and upload process finished.")

# Example of how to run this script directly (optional, for testing)
# if __name__ == "__main__":
#     # This block is for local testing and won't run in Airflow typically.
#     # For local testing, you might need to mock Airflow Variables or set them as env vars.
#     print("Running write_compare_price_report script directly for testing...")
#     # Mock Airflow Variables if needed for local run
#     class MockVariable:
#         def get(self, key, default_var=None):
#             print(f"MockVariable.get called for {key}")
#             if key == "minio_access_key": return os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
#             if key == "minio_secret_key": return os.environ.get("MINIO_SECRET_KEY", "minioadmin")
#             if key == "google_drive_folder_id": return os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "1EXRnLYXXxjc46nBcJ8DYKjEhv5zd-xfE")
#             return default_var
#     Variable = MockVariable()
#     create_comparison_report_and_upload()