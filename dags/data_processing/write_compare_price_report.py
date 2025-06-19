import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List

import polars as pl
import boto3
from airflow.models import Variable
from deltalake import DeltaTable
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Google Drive Configuration ---
SCOPES = ['https://www.googleapis.com/auth/drive']

def _authenticate_google_drive(service_account_file: str) -> Optional[Resource]:
    """
    Authenticates with Google Drive API using a service account.

    Args:
        service_account_file (str): Path to the GCP service account key JSON file.

    Returns:
        Optional[Resource]: The Google Drive API service object, or None on failure.
    """
    try:
        if not os.path.exists(service_account_file):
            logger.error(f"Service account key file not found at: {service_account_file}")
            raise FileNotFoundError(f"Service account key file not found at: {service_account_file}")

        creds = ServiceAccountCredentials.from_service_account_file(service_account_file, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)
        logger.info("Successfully authenticated with Google Drive API.")
        return service
    except Exception as e:
        logger.error(f"Failed to authenticate with Google Drive: {e}", exc_info=True)
        return None

def _share_file_with_user(service: Resource, file_id: str, email_address: str) -> None:
    """Shares a file on Google Drive with a specific user."""
    try:
        permission = {'type': 'user', 'role': 'reader', 'emailAddress': email_address}
        email_message = "A new price comparison report has been automatically generated and shared with you."
        service.permissions().create(
            fileId=file_id,
            body=permission,
            sendNotificationEmail=True,
            emailMessage=email_message,
            supportsAllDrives=True
        ).execute()
        logger.info(f"Successfully shared file {file_id} with {email_address}.")
    except HttpError as e:
        logger.error(f"An error occurred while sharing file {file_id}: {e}", exc_info=True)

def _upload_excel_to_drive(service: Resource, file_path: str, parent_folder_id: str, notification_email: str) -> Optional[Dict[str, str]]:
    """Uploads an Excel file to a specific folder in Google Drive."""
    if not service:
        logger.error("Google Drive service is not available. Cannot upload file.")
        return None

    try:
        # Check for write permissions on the parent folder
        folder = service.files().get(fileId=parent_folder_id, fields='id, name, capabilities').execute()
        if not folder.get('capabilities', {}).get('canAddChildren'):
            logger.error(f"Permission Denied: Service account cannot add files to folder '{folder.get('name')}' (ID: {parent_folder_id}).")
            return None
        logger.info(f"Verified write access to Google Drive folder: '{folder.get('name')}'")

        file_name = os.path.basename(file_path)
        file_metadata = {'name': file_name, 'parents': [parent_folder_id]}
        media = MediaFileUpload(file_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', resumable=True)

        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id,name,webContentLink,webViewLink',
            supportsAllDrives=True
        ).execute()

        file_id = file.get('id')
        logger.info(f"File '{file.get('name')}' uploaded to Google Drive with ID: {file_id}")

        # Share the file
        _share_file_with_user(service, file_id, notification_email)

        return {
            'file_id': file_id,
            'view_link': file.get('webViewLink'),
            'download_link': file.get('webContentLink')
        }
    except HttpError as e:
        logger.error(f"An error occurred during Google Drive upload: {e}", exc_info=True)
        return None

def _write_links_to_minio(file_details: Dict[str, str], bucket_name: str, object_name: str, storage_options: Dict[str, str]) -> None:
    """Writes Google Drive file details to a JSON file in MinIO."""
    if not file_details or not file_details.get('file_id'):
        logger.warning("No file details provided. Skipping write to MinIO.")
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
        s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=json_content)
        logger.info(f"Successfully wrote file links to MinIO: s3://{bucket_name}/{object_name}")
    except Exception as e:
        logger.error(f"Failed to write to MinIO: {e}", exc_info=True)

def generate_and_upload_price_comparison_report(
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
    source_bucket: str,
    google_drive_folder_id: str,
    gcp_service_account_file_path: str,
    notification_email: str,
    ingestion_date: datetime,
) -> Optional[Dict[str, Any]]:
    """
    Generates a price comparison report, uploads it to Google Drive, and logs metadata.

    This idempotent function performs the following steps:
    1.  Connects to MinIO and reads processed product data from a Delta table for a specific date.
    2.  Reads a base product name mapping file.
    3.  Filters and joins the data to find matching products from "Line Shopping" and "Watsons".
    4.  Calculates the price difference and creates a final comparison DataFrame.
    5.  If the comparison DataFrame is not empty, it saves it as an Excel file.
    6.  Authenticates with the Google Drive API using a service account.
    7.  Uploads the Excel file to a specified Google Drive folder.
    8.  Shares the uploaded file with a specified email address.
    9.  Writes the Google Drive file links (view, download) to a JSON file in MinIO.

    Args:
        minio_endpoint (str): The endpoint URL for the MinIO S3-compatible storage.
        minio_access_key (str): The access key for MinIO.
        minio_secret_key (str): The secret key for MinIO.
        source_bucket (str): The MinIO bucket containing the source data.
        google_drive_folder_id (str): The ID of the target folder in Google Drive.
        gcp_service_account_file_path (str): The local file path to the GCP service account JSON key.
        notification_email (str): The email address to notify upon successful upload.
        ingestion_date (datetime): The specific date for which to process the data.

    Returns:
        Optional[Dict[str, Any]]: A dictionary containing the results, including file paths and links,
                                  or None if the process fails or there is no data to compare.
    """
    logger.info("Starting price comparison report generation process.")
    
    storage_options = {
        "AWS_ACCESS_KEY_ID": minio_access_key,
        "AWS_SECRET_ACCESS_KEY": minio_secret_key,
        "AWS_ENDPOINT_URL": minio_endpoint,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_REGION": "us-east-1"
    }
    
    ingestion_date_str = ingestion_date.strftime('%Y-%m-%d')
    delta_path = f"s3://{source_bucket}/processed/product_delta"
    base_product_name_path = f"s3://{source_bucket}/cache/ecommerce_product_comparisons_final.csv"

    try:
        # --- 1. Read and Process Data ---
        logger.info(f"Reading data for ingestion date: {ingestion_date_str}")
        
        base_product_name_df = pl.read_csv(base_product_name_path, storage_options=storage_options)
        line_base_name_df = base_product_name_df.select("product_no", "query_product_name_from_line")
        watson_base_name_df = base_product_name_df.select("product_no", "base_product_name_from_watson")

        processed_df = pl.scan_delta(delta_path, storage_options=storage_options)
        daily_df = processed_df.filter(pl.col("ingest_timestamp_utc").dt.date() == ingestion_date.date()).collect()
        
        if daily_df.is_empty():
            logger.warning(f"No processed data found for date: {ingestion_date_str}. Aborting.")
            return None

        line_processed_df = daily_df.filter(pl.col("ecommerce_name") == 'line_shopping')
        watson_processed_df = daily_df.filter(pl.col("ecommerce_name") == 'watson')

        # --- 2. Match Products ---
        logger.info("Matching products based on base names.")
        line_base_names_to_join = line_base_name_df.rename({"query_product_name_from_line": "product_name"})
        watson_base_names_to_join = watson_base_name_df.rename({"base_product_name_from_watson": "product_name"})

        line_matching_df = line_processed_df.join(line_base_names_to_join.unique("product_name"), on="product_name", how="inner")
        watson_matching_df = watson_processed_df.join(watson_base_names_to_join.unique("product_name"), on="product_name", how="inner")
        
        logger.info(f"Line Shopping - Original: {line_processed_df.height}, Matched: {line_matching_df.height}")
        logger.info(f"Watsons - Original: {watson_processed_df.height}, Matched: {watson_matching_df.height}")

        # --- 3. Create Comparison DataFrame ---
        if watson_matching_df.is_empty() or line_matching_df.is_empty():
            logger.warning("No matching products found between sources. Cannot generate comparison report.")
            return None

        watson_to_join = watson_matching_df.select("product_no", "product_name", pl.col("sale_price").alias("price_from_watson"))
        line_to_join = line_matching_df.select("product_no", pl.col("sale_price").alias("price_from_line"))

        final_comparison_df = watson_to_join.join(line_to_join, on="product_no", how="left").with_columns(
            (pl.col("price_from_watson") - pl.col("price_from_line")).alias("diff")
        ).select("product_no", "product_name", "price_from_line", "price_from_watson", "diff")

        if final_comparison_df.is_empty():
            logger.info("Comparison DataFrame is empty after join. No report will be generated.")
            return None
        
        logger.info(f"Generated comparison report with {final_comparison_df.height} rows.")
        logger.info(f"Comparison head:\n{final_comparison_df.head()}")

        # --- 4. Save, Upload, and Share Report ---
        current_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        excel_file_path = f'/tmp/product_comparison_{current_timestamp}.xlsx'
        minio_links_path = f'metadata/gdrive_links/product_comparison_{current_timestamp}.json'

        final_comparison_df.write_excel(excel_file_path)
        logger.info(f"Saved comparison data to temporary Excel file: {excel_file_path}")

        service = _authenticate_google_drive(gcp_service_account_file_path)
        if not service:
            raise ConnectionError("Failed to authenticate with Google Drive. Aborting upload.")

        gdrive_file_details = _upload_excel_to_drive(service, excel_file_path, google_drive_folder_id, notification_email)

        if gdrive_file_details:
            _write_links_to_minio(gdrive_file_details, source_bucket, minio_links_path, storage_options)
            
            # Clean up local file
            os.remove(excel_file_path)
            logger.info(f"Removed temporary file: {excel_file_path}")

            return {
                "status": "Success",
                "report_path_local": excel_file_path,
                "report_path_minio_metadata": f"s3://{source_bucket}/{minio_links_path}",
                "gdrive_details": gdrive_file_details,
                "rows_compared": final_comparison_df.height
            }
        else:
            logger.error("Failed to upload file to Google Drive. No metadata will be written to MinIO.")
            return None

    except Exception as e:
        logger.error(f"An unexpected error occurred in the report generation pipeline: {e}", exc_info=True)
        return None

def main():
    """
    Main function to execute the script from the command line or as an Airflow task.
    Retrieves configuration from Airflow Variables with local fallbacks.
    """
    logger.info("Executing main function to generate price comparison report.")
    
    # --- Configuration ---
    try:
        minio_access_key = Variable.get("minio_access_key")
        minio_secret_key = Variable.get("minio_secret_key")
        gcp_service_account_path = Variable.get("gcp_service_account_file_path", default_var='/opt/airflow/private_key.json')
        logger.info("Successfully retrieved credentials from Airflow Variables.")
    except Exception:
        logger.warning("Could not retrieve credentials from Airflow Variables. Using local defaults.")
        minio_access_key = "minioadmin"
        minio_secret_key = "minioadmin"
        gcp_service_account_path = os.getenv('GCP_SERVICE_ACCOUNT_FILE_PATH', 'private_key.json')

    config = {
        "minio_endpoint": "http://minio:9000",
        "minio_access_key": minio_access_key,
        "minio_secret_key": minio_secret_key,
        "source_bucket": "data-pipeline-demo",
        "google_drive_folder_id": "1EXRnLYXXxjc46nBcJ8DYKjEhv5zd-xfE",
        "gcp_service_account_file_path": gcp_service_account_path,
        "notification_email": "beyondbegin41@gmail.com",
        "ingestion_date": datetime.utcnow()
    }

    logger.info(f"Configuration loaded: {json.dumps({k:v for k,v in config.items() if k != 'minio_secret_key'}, indent=2)}")

    result = generate_and_upload_price_comparison_report(**config)

    if result:
        logger.info("Process completed successfully.")
        logger.info(f"Result: {json.dumps(result, indent=2)}")
    else:
        logger.error("Process failed or completed with no data.")

if __name__ == "__main__":
    main()