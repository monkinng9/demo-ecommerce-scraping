import polars as pl
from datetime import datetime
import re
import json
import s3fs
from airflow.models import Variable
from deltalake import write_deltalake, DeltaTable

def process_pre_landing_watson_data():
    """
    This script connects to a MinIO S3-compatible object storage to process
    JSON files from a specified pre-landing directory. It extracts data from
    files with names starting with 'watson*', transforms the data into a
    Polars DataFrame, adds ingestion metadata, and writes the result to a
    Delta table, partitioned by year and month.

    The script is designed to be executed within an Airflow environment,
    retrieving MinIO credentials from Airflow Variables.
    """

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
    source_prefix = f'pre_landing/{ingestion_date_str}/'
    target_path = f"s3://{source_bucket}/processed/product_delta"


    # --- S3 File System and Data Loading ---
    try:
        s3 = s3fs.S3FileSystem(
            key=minio_access_key,
            secret=minio_secret_key,
            client_kwargs={'endpoint_url': minio_endpoint}
        )

        # Find all files in the source directory matching the pattern
        source_path = f"{source_bucket}/{source_prefix}"
        all_files = s3.glob(f"{source_path}watson*")

        if not all_files:
            print(f"No files starting with 'watson' found in s3://{source_path}")
            return

        print(f"Found {len(all_files)} files to process.")

    except Exception as e:
        print(f"Failed to connect to S3 or list files. Error: {e}")
        return


    # --- Data Extraction and Transformation ---
    all_products_data = []

    for file_path in all_files:
        print(f"Processing file: {file_path}")
        try:
            with s3.open(file_path, 'r', encoding='utf-8') as f:
                json_data_string = f.read()
                data = json.loads(json_data_string)

            # Logic adapted from read_watson_data.py
            product_list = data.get('products', [])
            if not isinstance(product_list, list):
                print(f"Warning: Product list not found or not in expected format in {file_path}.")
                continue

            for product in product_list:
                product_name = product.get('name')
                if not product_name:
                    continue

                # Safely get nested price value
                sale_price = product.get('price', {}).get('value')
                cleaned_price = None
                if sale_price is not None:
                    price_str = str(sale_price)
                    # Regex to remove currency symbols and commas
                    cleaned_price_str = re.sub(r'[฿$,€]', '', price_str).strip()
                    try:
                        cleaned_price = float(cleaned_price_str)
                    except (ValueError, TypeError):
                        cleaned_price = None # Leave as null if conversion fails

                all_products_data.append({
                    "product_name": product_name,
                    "sale_price": cleaned_price,
                })

        except json.JSONDecodeError:
            print(f"Error decoding JSON from file: {file_path}")
        except Exception as e:
            print(f"An error occurred while processing file {file_path}: {e}")

    if not all_products_data:
        print("No valid product data extracted from any of the files. Exiting.")
        return

    # --- Create Polars DataFrame and Add New Fields ---
    schema = {
        "product_name": pl.Utf8,
        "sale_price": pl.Float64
    }
    df = pl.DataFrame(all_products_data, schema=schema)

    # Add new required fields
    df = df.with_columns([
        pl.lit(ingestion_date).alias("ingest_timestamp_utc"),
        pl.lit("watson").alias("ecommerce_name"),
        pl.lit(ingestion_date.year).alias("year"),
        pl.lit(ingestion_date.month).alias("month")
    ])

    print("Successfully created DataFrame with new fields:")
    print(df.head())


    # --- Write to Delta Lake on S3 ---
    # Define storage options specifically for Delta Lake (delta-rs)
    delta_lake_storage_options = {
        "AWS_ACCESS_KEY_ID": minio_access_key,
        "AWS_SECRET_ACCESS_KEY": minio_secret_key,
        "AWS_ENDPOINT_URL": minio_endpoint,  # For delta-rs with S3-compatible storage
        "AWS_ALLOW_HTTP": "true",  # Necessary if endpoint is HTTP, like MinIO's default
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",  # Often required for S3-compatible storage like MinIO
        "AWS_REGION": "us-east-1"  # Optional: Can be a dummy value if not applicable but sometimes helps
    }

    table_exists = False
    try:
        print(f"Checking if Delta table exists at: {target_path}")
        # Attempt to load the table to check for existence
        DeltaTable(target_path, storage_options=delta_lake_storage_options)
        table_exists = True
        print(f"Delta table already exists at {target_path}.")
    except Exception:  # Broad exception catch, as specific error for non-existence can vary
        print(f"Delta table does not exist at {target_path} or failed to load. It will be created.")
        table_exists = False

    try:
        print(f"Writing data to Delta table at: {target_path}")
        arrow_table = df.to_arrow()
        write_deltalake(
            target_path,
            arrow_table,
            storage_options=delta_lake_storage_options,
            partition_by=["year", "month"],
            mode="append"  # Creates if not exists, appends if exists
        )
        print("Successfully wrote data to Delta table.")

        if not table_exists:
            print(f"Performing Z-order optimization on newly created table: {target_path} for column 'ingest_timestamp_utc'")
            dt = DeltaTable(target_path, storage_options=delta_lake_storage_options)
            dt.optimize.z_order(["ingest_timestamp_utc"])
            print("Successfully optimized Delta table with Z-order.")
        
    except Exception as e:
        print(f"Failed during Delta Lake write or optimization. Error: {e}")


if __name__ == "__main__":
    # This allows the script to be run locally for testing
    # In a real Airflow DAG, you would call process_pre_landing_watson_data()
    # from a PythonOperator.
    # Note: For local execution, ensure you have credentials or fallbacks set up.
    process_pre_landing_watson_data()
