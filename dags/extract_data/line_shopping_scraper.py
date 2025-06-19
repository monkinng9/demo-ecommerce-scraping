import requests
import os
import json
from datetime import datetime, timezone
from typing import Optional
from airflow.models import Variable
import boto3

def fetch_line_shopping_products(search_query: str, shops_page: int = 1, limit: int = 200) -> Optional[dict]:
    """
    Fetches product and shop data from the Line Shopping API based on a search query.

    Args:
        search_query (str): The search term for products/shops (e.g., "eucerin").
        shops_page (int): The page number for shop results pagination.
        limit (int): The number of items to retrieve per page for products/shops.

    Returns:
        Optional[dict]: A dictionary containing the API response (JSON) if successful,
                     otherwise None.
    """
    url = "https://customer-api.line-apps.com/search/graph"

    headers = {
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Origin': 'https://shop.line.me',
        'Referer': 'https://shop.line.me/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'Sec-GPC': '1',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
        'accept': '*/*',
        'content-type': 'application/json',
        'sec-ch-ua': '"Brave";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"'
    }

    # GraphQL query template
    query_template = """
    query SearchWithCouponQuery($limit: Int!, $shopsPage: Int!, $search: String!, $productPaymentTypes: [PaymentType], $hasTrustBadge: Boolean, $isGift: Boolean, $hasEReceipt: Boolean, $productSkipShopIds: [Int!], $productLastSorts: [Float!], $categoryId: Int, $isBoostShippingCoupon: Boolean) {
      products(
        limit: $limit
        input: {search: $search}
        paymentTypes: $productPaymentTypes
        hasTrustBadge: $hasTrustBadge
        isGift: $isGift
        hasEReceipt: $hasEReceipt
        skipShopIds: $productSkipShopIds
        lastSorts: $productLastSorts
        categoryId: $categoryId
        isBoostShippingCoupon: $isBoostShippingCoupon
      ) {
        products {
          id
          name
          imageUrl
          discountPercent
          price
          salePrice
          storefrontUrl
          isInStock
          hasRLP
          isGift
          hasEReceipt
          hasTrustBadge
          shippingCouponBadge
          shop {
            id
            name
            imageUrl
            hasRLP
            storefrontUrl
            friendCount
            __typename
          }
          __typename
        }
        lastSorts
        lastPage
        __typename
      }
      shops(
        limit: $limit
        page: $shopsPage
        input: {search: $search}
        hasTrustBadge: $hasTrustBadge
        hasGift: $isGift
        hasEReceipt: $hasEReceipt
        isBoostShippingCoupon: $isBoostShippingCoupon
      ) {
        totalPage
        totalShop
        shops {
          id
          name
          imageUrl
          hasRLP
          hasGift
          hasEReceipt
          storefrontUrl
          friendCount
          searchId
          brandType
          hasTrustBadge
          shippingCouponBadge
          products {
            id
            name
            imageUrl
            discountPercent
            price
            salePrice
            storefrontUrl
            isGift
            __typename
          }
          __typename
        }
        __typename
      }
      topCategories(limit: 5, input: {search: $search}, preferCategoryId: $categoryId) {
        id
        nameTH
        __typename
      }
    }
    """

    payload = {
        "operationName": "SearchWithCouponQuery",
        "variables": {
            "limit": limit,
            "shopsPage": shops_page,
            "search": search_query,
            "productPaymentTypes": [],
            "hasTrustBadge": False,
            "isGift": False,
            "hasEReceipt": False,
            "productSkipShopIds": [],
            "productLastSorts": [],
            "isBoostShippingCoupon": True # As per cURL
            # categoryId is omitted as it's optional and not in the base cURL vars
        },
        "query": query_template
    }

    try:
        minio_access_key = Variable.get("minio_access_key")
        minio_secret_key = Variable.get("minio_secret_key")
        minio_endpoint = Variable.get("minio_endpoint", default_var="minio:9000")
    except KeyError as e:
        print(f"Error: Airflow variable {e} not set.")
        return None

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        
        print(f"Status Code for '{search_query}' page {shops_page}: {response.status_code}")
        response_data = response.json()

        # Add ingest timestamp and ecommerce name
        response_data['ingest_timestamp_utc'] = datetime.now(timezone.utc).isoformat()
        response_data['ecommerce_name'] = "line_shopping"

        # Boto3 S3 client initialization for Minio
        s3_client = boto3.client(
            's3',
            endpoint_url=f'http://{minio_endpoint}',
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key
        )

        # Generate object name for S3
        ecommerce_name = "line_shopping"
        store_name = search_query
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
        print(f"HTTP error occurred for '{search_query}': {http_err}")
        print(f"Response content: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred for '{search_query}': {req_err}")
    except json.JSONDecodeError:
        print(f"Failed to decode JSON response for '{search_query}'.")
        print(f"Response content: {response.text}")
    return None

if __name__ == "__main__":
    search_terms = ["vistra", "blackmores", "dettol", "swisse", "eucerin"]
    for term in search_terms:
        print(f"\nFetching Line Shopping data for query: '{term}'")
        # Fetch first page for shops
        data = fetch_line_shopping_products(search_query=term, shops_page=1)
        if data:
            # You can add logic here to check data.data.shops.totalPage 
            # and loop through more pages if needed.
            # For example, to get total products:
            if data.get('data') and data['data'].get('products') and data['data']['products'].get('products'):
                 print(f"Found {len(data['data']['products']['products'])} products in the first batch for '{term}'.")
            if data.get('data') and data['data'].get('shops') and data['data']['shops'].get('shops'):
                 print(f"Found {len(data['data']['shops']['shops'])} shops in the first batch for '{term}'.")
                 print(f"Total shop pages available: {data['data']['shops'].get('totalPage', 0)}")
        else:
            print(f"Failed to fetch data for '{term}' from Line Shopping.")
