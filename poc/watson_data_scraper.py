import requests
import os
from dotenv import load_dotenv
import json
from datetime import datetime, timezone
from typing import Optional

# Load environment variables from .env file
load_dotenv()

# --- Path Configuration ---
# Get the absolute path of the directory where the script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Construct the path to the 'data' directory (assuming it's one level up from SCRIPT_DIR)
DATA_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..', 'data'))
# Ensure the data directory exists, create if not
os.makedirs(DATA_DIR, exist_ok=True)

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

    bearer_token = os.getenv("WATSONS_BEARER_TOKEN")
    if not bearer_token:
        print("Error: WATSONS_BEARER_TOKEN environment variable not set.")
        print("Please create a .env file with WATSONS_BEARER_TOKEN='your_token_here'")
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


        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        store_name = query
        page_num = response_data.get("pagination", {}).get("currentPage", 0)
        filename = os.path.join(DATA_DIR, f"{timestamp}_watson_{store_name}_{page_num}.json")

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
        # Fetch the first page to get pagination details
        initial_data = fetch_watsons_products(query, page=0)

        if initial_data and 'pagination' in initial_data:
            total_pages = initial_data.get('pagination', {}).get('totalPages', 1)
            print(f"Found {initial_data.get('pagination', {}).get('totalResults', 0)} results across {total_pages} pages for '{query}'.")

            # Loop through the rest of the pages if there are more than one
            if total_pages > 1:
                for page_num in range(1, total_pages):
                    print(f"--- Fetching page {page_num + 1} of {total_pages} for '{query}' ---")
                    fetch_watsons_products(query, page=page_num)
        else:
            print(f"Failed to fetch initial data or pagination info for '{query}'.")
