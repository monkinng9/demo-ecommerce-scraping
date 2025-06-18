import requests
import os
import json
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DATA_DIR = "data"

def fetch_line_shopping_products(search_query: str, shops_page: int = 1, limit: int = 200) -> dict | None:
    """
    Fetches product and shop data from the Line Shopping API based on a search query.

    Args:
        search_query (str): The search term for products/shops (e.g., "eucerin").
        shops_page (int): The page number for shop results pagination.
        limit (int): The number of items to retrieve per page for products/shops.

    Returns:
        dict | None: A dictionary containing the API response (JSON) if successful,
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
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        
        print(f"Status Code for '{search_query}' page {shops_page}: {response.status_code}")
        response_data = response.json()

        # Add ingest timestamp and ecommerce name
        response_data['ingest_timestamp_utc'] = datetime.now(timezone.utc).isoformat()
        response_data['ecommerce_name'] = "line_shopping"

        # Create data directory if it doesn't exist
        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)

        # Generate filename
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Sanitize search_query for filename
        safe_search_query = "".join(c if c.isalnum() else "_" for c in search_query)
        filename = os.path.join(DATA_DIR, f"line_{timestamp_str}_{safe_search_query}_{shops_page}.json")

        # Save data to JSON file
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(response_data, f, ensure_ascii=False, indent=4)
            print(f"Data saved to {filename}")
        except IOError as e:
            print(f"Error saving data to {filename}: {e}")
            
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
