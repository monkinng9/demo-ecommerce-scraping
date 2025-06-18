import json
import polars as pl
import re

# This script parses a JSON file to extract product information into a Polars DataFrame.
# It handles nested JSON structures, missing data, and cleans the data before final output.

def parse_product_data(json_file_path: str) -> pl.DataFrame:
    """
    Parses a JSON file to extract product information into a Polars DataFrame.

    Args:
        json_file_path (str): The path to the JSON file.

    Returns:
        pl.DataFrame: A DataFrame containing product names and sale prices.
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            json_data_string = f.read()
        data = json.loads(json_data_string)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"Error reading or parsing JSON file: {e}")
        return pl.DataFrame({"product_name": [], "sale_price": []})

    # Find the list of products, which might be nested
    product_list = data.get('products', {})

    if not isinstance(product_list, list):
        print("Product list not found or not in expected format.")
        return pl.DataFrame({"product_name": [], "sale_price": []})

    # --- Data Extraction and Cleaning ---
    extracted_data = []
    for product in product_list:
        # 1. Extract product name (skip record if missing)
        product_name = product.get('name')
        if not product_name:
            continue

        # 2. Extract sale price
        sale_price = product.get('price').get('value')

        # 3. Clean and prepare sale price
        cleaned_price = None
        if sale_price is not None:
            # Remove currency symbols (like '฿', '$', '€') and commas
            price_str = str(sale_price)
            # This regex removes common currency symbols and commas
            cleaned_price_str = re.sub(r'[฿$,€]', '', price_str).strip()
            try:
                cleaned_price = float(cleaned_price_str)
            except (ValueError, TypeError):
                # If conversion fails, leave it as null
                cleaned_price = None

        extracted_data.append({
            "product_name": product_name,
            "sale_price": cleaned_price,
            "ecommerce": "watson"
        })

    # --- Create Polars DataFrame ---
    # Define the schema to ensure correct data types
    schema = {
        "product_name": pl.Utf8,
        "sale_price": pl.Float64,
        "ecommerce": pl.Utf8
    }


    # Create the DataFrame
    df = pl.DataFrame(extracted_data, schema=schema)

    return df

if __name__ == "__main__":
    # Path to the JSON file
    json_file = '../data/watson_eucerin.json'

    # Get the final DataFrame
    product_df = parse_product_data(json_file)

    # Define the output CSV file path
    output_csv_file = '../data/watson_eucerin_products.csv'

    # Write the DataFrame to a CSV file
    try:
        product_df.write_csv(output_csv_file)
        print(f"DataFrame successfully written to {output_csv_file}")
    except Exception as e:
        print(f"Error writing DataFrame to CSV: {e}")
