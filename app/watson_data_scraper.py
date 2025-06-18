import requests
import os
from dotenv import load_dotenv
import json
from datetime import datetime, timezone

# Load environment variables from .env file
load_dotenv()

def fetch_watsons_products(query: str, page_size: int = 200) -> dict | None:
    """
    Fetches product data from the Watsons API based on a search query.

    Args:
        query (str): The search term for products (e.g., "eucerin").
        page_size (int): The number of products to retrieve per page.

    Returns:
        dict | None: A dictionary containing the API response (JSON) if successful,
                     otherwise None.
    """
    base_url = "https://api.watsons.co.th/api/v2/wtcth/products/search"
    
    params = {
        "fields": "FULL",
        "query": query,
        "pageSize": page_size,
        "sort": "mostRelevant",
        "useDefaultSearch": "false",
        "brandRedirect": "true",
        "ignoreSort": "false",
        "lang": "en_TH",
        "curr": "THB"
    }

    bearer_token = os.getenv("WATSONS_BEARER_TOKEN")
    if not bearer_token:
        print("Error: WATSONS_BEARER_TOKEN environment variable not set.")
        print("Please create a .env file with WATSONS_BEARER_TOKEN='your_token_here'")
        return None

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.7',
        'authorization': f'bearer {bearer_token}',
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

        # Create data directory if it doesn't exist
        data_dir = "data"
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        store_name = query
        # Attempt to get page number from response, default to 1 if not found
        page_num = response_data.get("pagination", {}).get("currentPage", 1)
        if page_num is None: # Handle cases where currentPage might be explicitly None
            page_num = 1
            
        filename = os.path.join(data_dir, f"watson_{timestamp}_{store_name}_{page_num}.json")

        # Save data to JSON file
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(response_data, f, ensure_ascii=False, indent=4)
            print(f"Data saved to {filename}")
        except IOError as e:
            print(f"Error saving data to {filename}: {e}")
            
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

if __name__ == "__main__":
    search_queries = ["vistra", "blackmores", "dettol", "swisse", "eucerin"]
    for query in search_queries:
        print(f"\nFetching data for query: '{query}'")
        product_data = fetch_watsons_products(query)

        if product_data:
            # Example: Print the number of products found
            if 'products' in product_data and isinstance(product_data['products'], list):
                print(f"Found {len(product_data['products'])} products for '{query}'.")
            elif 'results' in product_data and isinstance(product_data['results'], list): # Alternative key for products
                print(f"Found {len(product_data['results'])} products for '{query}'.")
            else:
                # Check for pagination totalResults for a count if products list is not directly available
                total_results = product_data.get("pagination", {}).get("totalResults", 0)
                if total_results > 0:
                    print(f"Found {total_results} products for '{query}' (based on pagination total). ")
                else:
                    print(f"Could not determine the number of products for '{query}' from common keys.")
        else:
            print(f"Failed to fetch data for '{query}'.")
