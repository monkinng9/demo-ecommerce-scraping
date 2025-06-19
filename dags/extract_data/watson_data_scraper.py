import requests
import os
import json
from datetime import datetime, timezone
from typing import Optional
from airflow.models import Variable
import boto3

def fetch_watsons_products(query: str, page: int = 0, page_size: int = 200) -> Optional[dict]:
    """
    Fetches product data from the Watsons API based on a search query.

    Args:
        query (str): The search term for products (e.g., "eucerin").
        page_size (int): The number of products to retrieve per page.

    Returns:
        Optional[dict]: A dictionary containing the API response (JSON) if successful,
                     otherwise None.
    """
    base_url = "https://api.watsons.co.th/api/v2/wtcth/products/search"
    
    params = {
        "fields": "FULL",
        "query": query,
        "pageSize": page_size,
        "currentPage": page,
        "sort": "mostRelevant",
        "useDefaultSearch": "false",
        "brandRedirect": "true",
        "ignoreSort": "false",
        "lang": "en_TH",
        "curr": "THB"
    }

    try:
        bearer_token = Variable.get("WATSONS_BEARER_TOKEN")
        minio_access_key = Variable.get("minio_access_key")
        minio_secret_key = Variable.get("minio_secret_key")
        minio_endpoint = Variable.get("minio_endpoint", default_var="minio:9000")
    except KeyError as e:
        print(f"Error: Airflow variable {e} not set.")
        return None

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.7',
        'authorization': f'{bearer_token}',
        'cache-control': 'no-cache, no-store, must-revalidate, post-check=0, pre-check=0',
        'expires': '0',
        'origin': 'https://www.watsons.co.th',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'queue-target': f'https://www.watsons.co.th/en/search?text={query}&useDefaultSearch=false&brandRedirect=true',
        'queueit-target': f'https://www.watsons.co.th/en/search?text={query}&useDefaultSearch=false&brandRedirect=true',
        'referer': 'https://www.watsons.co.th/',
        'sec-ch-ua': '"Brave";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'sec-gpc': '1',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
        'vary': '*'
    }

    try:
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
        
        print(f"Status Code: {response.status_code}")
        response_data = response.json()

        # Add ingest timestamp and ecommerce name
        response_data['ingest_timestamp_utc'] = datetime.now(timezone.utc).isoformat()
        response_data['ecommerce_name'] = "watsons"

        # Boto3 S3 client initialization for Minio
        s3_client = boto3.client(
            's3',
            endpoint_url=f'http://{minio_endpoint}',
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key
        )

        # Generate object name for S3
        ecommerce_name = "watsons"
        store_name = query
        current_date = datetime.utcnow().strftime('%Y-%m-%d')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        object_name = f"pre_landing/{current_date}/{ecommerce_name}_{store_name}_{timestamp}.json"

        # Convert response data to bytes
        json_data = json.dumps(response_data, ensure_ascii=False, indent=4).encode('utf-8')

        # Upload to S3 (Minio)
        bucket_name = "data-pipeline-demo"
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' already exists.")
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                try:
                    s3_client.create_bucket(Bucket=bucket_name)
                    print(f"Bucket '{bucket_name}' created.")
                except Exception as create_err:
                    print(f"Error creating bucket: {create_err}")
                    return None
            else:
                print(f"Error checking bucket existence: {e}")
                return None

        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=json_data,
            ContentType="application/json"
        )
        print(f"Data saved to S3 bucket '{bucket_name}' with object name '{object_name}'")
            
        return response_data
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response content: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    except json.JSONDecodeError:
        print("Failed to decode JSON response.")
        print(f"Response content: {response.text}")
    return None

