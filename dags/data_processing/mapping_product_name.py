"""
This script performs product name mapping and similarity comparison between two e-commerce datasets (Line Shopping and Watsons).
It utilizes OpenAI embeddings for semantic similarity and GPT-4o-mini for verification of product matches.

Configuration:
- MinIO credentials (access key, secret key) are retrieved from Airflow Variables. If not found, dummy values are used for local testing.
- MinIO endpoint is set to "http://minio:9000".
- Data is read from an S3 Delta Lake path: `s3://data-pipeline-demo/processed/product_delta`.
- OpenAI API key is retrieved from Airflow Variables.

Main Steps:
1.  **Setup and Helper Functions**: Initializes the OpenAI client and defines functions for generating and caching text embeddings.
2.  **Data Loading and Preparation**: Reads product data from Delta Lake, filters it into Line Shopping and Watsons DataFrames, and generates embeddings for Watsons products.
3.  **Core Matching Logic**: Implements functions to find the best matches between products using cosine similarity of embeddings and to verify these matches using GPT-4o-mini.
4.  **Product Processing and DataFrame Creation**: Processes each product row from the Line Shopping DataFrame in parallel, finding and verifying matches against the Watsons product data. Results are compiled into a Polars DataFrame.
5.  **Save Results to Excel**: Filters the results based on similarity and verification status, calculates price differences, and saves the final comparison DataFrame to an Excel file.
"""

import polars as pl
from datetime import datetime
import re
import json
from airflow.models import Variable
from openai import OpenAI
import pickle
import numpy as np
from tqdm import tqdm
import concurrent.futures
import time
import os
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

delta_lake_storage_options = {
    "AWS_ACCESS_KEY_ID": minio_access_key,
    "AWS_SECRET_ACCESS_KEY": minio_secret_key,
    "AWS_ENDPOINT_URL": minio_endpoint,  # For delta-rs with S3-compatible storage
    "AWS_ALLOW_HTTP": "true",  # Necessary if endpoint is HTTP, like MinIO's default
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",  # Often required for S3-compatible storage like MinIO
    "AWS_REGION": "us-east-1"  # Optional: Can be a dummy value if not applicable but sometimes helps
}

ldf = pl.read_delta(delta_path, storage_options=delta_lake_storage_options)

line_df = ldf.filter(pl.col("ecommerce_name") == 'line_shopping')

watson_df = ldf.filter(pl.col("ecommerce_name") == 'watson')


# --- 1. SETUP AND HELPER FUNCTIONS ---

# Initialize OpenAI client
# Ensure OPENAI_API_KEY environment variable is set
client = OpenAI(api_key=Variable.get("OPENAI_API_KEY"))

# Define function: generate text embeddings
def generate_embeddings(text):
    """
    Generate embeddings for input text using OpenAI's text-embedding-3-small model.

    Args:
        text (str or list[str]): The text or list of texts to embed.

    Returns:
        list or list[list]: A list of embeddings, or a list of lists of embeddings if input was a list.
    """
    if not text:
        return []
    
    response = client.embeddings.create(
        input=text,
        model="text-embedding-3-small"
    )
    embeddings = [record.embedding for record in response.data]
    return embeddings if isinstance(text, list) else embeddings[0]

# Set embedding cache path
embedding_cache_path = "embeddings_cache.pkl"

# Try to load cached embeddings
try:
    with open(embedding_cache_path, 'rb') as f:
        embedding_cache = pickle.load(f)
except FileNotFoundError:
    embedding_cache = {}

# Define function: obtain text embeddings through caching mechanism
def embedding_from_string(string: str, embedding_cache=embedding_cache) -> list:
    """
    Get embedding for given text, using cache mechanism to avoid recomputation.

    Args:
        string (str): The input text to get the embedding for.
        embedding_cache (dict): A dictionary used as a cache for embeddings.

    Returns:
        list: The embedding vector for the input string.
    """
    if string not in embedding_cache.keys():
        embedding_cache[string] = generate_embeddings(string)
        with open(embedding_cache_path, "wb") as embedding_cache_file:
            pickle.dump(embedding_cache, embedding_cache_file)
    return embedding_cache[string]

df_base = watson_df

df_query = line_df

print("Preparing base by generating embeddings for all Watson's products...")
# Get all product names from the base dataframe
base_names = df_base.select('product_name').to_series().to_list()
# Generate and cache embeddings for all base product names
base_embeddings = [embedding_from_string(name) for name in base_names]
# Convert to a NumPy array for fast vector operations
base_embeddings_np = np.array(base_embeddings)
print(f"base prepared. {len(base_embeddings_np)} product embeddings are loaded.\n")


# --- 3. CORE MATCHING LOGIC ---
def find_best_match_in_context(query_name, context_embeddings_np, top_n=3):
    """
    Finds the top N most similar products in the context catalog for a given query name.

    Args:
        query_name (str): The name of the product to query.
        context_embeddings_np (numpy.ndarray): NumPy array of embeddings for products in the context.
        top_n (int): The number of top similar products to return.

    Returns:
        tuple: A tuple containing:
               - top_n_indices (numpy.ndarray): Indices of the top N most similar products in the context.
               - top_n_similarities (numpy.ndarray): Cosine similarity scores for the top N products.
    """
    # Generate embedding for the query name
    query_embedding = embedding_from_string(query_name)

    # Calculate cosine similarity between the query embedding and all context embeddings
    similarities = np.dot(context_embeddings_np, query_embedding) / \
                   (np.linalg.norm(context_embeddings_np, axis=1) * np.linalg.norm(query_embedding))

    # Get the indices of the top N most similar products
    top_n_indices = np.argsort(similarities)[::-1][:top_n]
    top_n_similarities = similarities[top_n_indices]

    return top_n_indices, top_n_similarities

def verify_products_similarity(base_product, product_list_to_compare):
    """
    Uses GPT-4o-mini to verify if any product in a list is the same as a base product.

    Args:
        base_product (str): The name of the base product.
        product_list_to_compare (list): A list of product names to compare against the base product.

    Returns:
        tuple: A tuple containing the matched product name and a boolean indicating if a match was found.
    """
    if not product_list_to_compare:
        return None, False

    # Construct the prompt for GPT-4o-mini
    product_list_str = "\n".join([f"- {p}" for p in product_list_to_compare])
    prompt = f"""Given the base product: '{base_product}', is any of the following products the same product? If yes, return the exact name of the product from the list. If no, return 'None'.\n\nProduct List:\n{product_list_str}\n\nExact Product Name or 'None':"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that identifies matching products."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=50,
            temperature=0.0
        )
        content = response.choices[0].message.content.strip()
        if content != 'None' and content in product_list_to_compare:
            return content, True
        else:
            return None, False
    except Exception as e:
        print(f"Error during GPT-4o-mini verification: {e}")
        return None, False


# --- 4. PROCESS PRODUCTS AND CREATE DATAFRAME WITH COMPARISON RESULTS ---

def process_product_row(row, context_df, context_embeddings_np):
    """
    Processes a single product row to find and verify matches with retry logic.

    This function attempts to find the best match for a given product from a
    context dataset. It performs the match and verification process up to 3
    times in case of transient errors. It uses embedding similarity to find
    potential matches and then a GPT-4o-mini call for final verification.

    Args:
        row (polars.DataFrame.row): A single row from the query DataFrame,
                                    expected to contain 'product_name' and 'sale_price'.
        context_df (polars.DataFrame): The DataFrame containing products to match against.
                                       Expected to have 'product_name' and 'sale_price'.
        context_embeddings_np (numpy.ndarray): NumPy array of embeddings for products in `context_df`.

    Returns:
        dict: A dictionary containing the comparison results:
              - 'base_product_name (from line)': Product name from the query row.
              - 'base_product_name (from watson)': Matched product name from context, or 'Matching Failed'.
              - 'price_from_line': Sale price from the query row.
              - 'price_from_watson': Sale price from the matched context product, or 0.
              - 'similarity': Cosine similarity score of the best match, or 0.0.
              - 'verified_match': Boolean indicating if a verified match was found.
              If processing fails after all retries, it returns a dictionary
              with 'verified_match' set to False and placeholder values.
    """
    query_product_name = row.get('product_name')
    query_product_price = row.get('sale_price')

    if not query_product_name:
        return None

    for attempt in range(3):
        try:
            # Find best matches based on embeddings
            match_indices, similarity_scores = find_best_match_in_context(query_product_name, context_embeddings_np)
            matched_products = [context_df.row(i, named=True) for i in match_indices]
            matched_product_names = [p['product_name'] for p in matched_products]

            # Verify the matches
            verified_product_name, is_match = verify_products_similarity(query_product_name, matched_product_names)

            if is_match:
                verified_product_index_in_matches = matched_product_names.index(verified_product_name)
                original_match_index = match_indices[verified_product_index_in_matches]
                similarity_score = similarity_scores[verified_product_index_in_matches]
                matched_product_info = context_df.row(original_match_index, named=True)
                
                return {
                    'base_product_name (from line)': query_product_name,
                    'base_product_name (from watson)': verified_product_name,
                    'price_from_line': query_product_price,
                    'price_from_watson': matched_product_info['sale_price'],
                    'similarity': similarity_score,
                    'verified_match': True
                }
            else:
                top_match_info = matched_products[0]
                return {
                    'base_product_name (from line)': query_product_name,
                    'base_product_name (from watson)': top_match_info['product_name'],
                    'price_from_line': query_product_price,
                    'price_from_watson': top_match_info['sale_price'],
                    'similarity': similarity_scores[0],
                    'verified_match': False
                }
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {row.get('product_name', 'Unknown')}: {e}")
            if attempt < 2:
                time.sleep(5)
    
    # Return a default/error structure if all retries fail
    return {
        'base_product_name (from line)': row.get('product_name', 'Unknown'),
        'base_product_name (from watson)': 'Matching Failed',
        'price_from_line': row.get('sale_price', 0),
        'price_from_watson': 0,
        'similarity': 0.0,
        'verified_match': False
    }

# --- Main processing using concurrent futures ---

results_list = []

print("Finding matches and building DataFrame with 5 workers...")

# Use ThreadPoolExecutor to process products in parallel
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    # Map the processing function to each row of the query dataframe
    future_to_row = {executor.submit(process_product_row, row, df_base, base_embeddings_np): row for row in df_query.iter_rows(named=True)}
    
    # Process futures as they complete and show progress with tqdm
    for future in tqdm(concurrent.futures.as_completed(future_to_row), total=len(df_query), desc="Processing products"):
        try:
            result = future.result()
            if result:
                results_list.append(result)
        except Exception as exc:
            print(f'A product row generated an exception: {exc}')

# Convert the list of results into a polars DataFrame
df_results = pl.DataFrame(results_list)
filtered_df_results = df_results.filter((pl.col("similarity") >= 0.8) & (pl.col("verified_match") == True))
filtered_df_results = filtered_df_results.with_columns([
    (pl.col('price_from_line') / pl.col('price_from_watson')).alias('diff')
])

# Display the final DataFrame
print("\n--- Product Comparison DataFrame ---")
print(filtered_df_results)

# --- 5. SAVE RESULTS TO EXCEL ---

# Define the output Excel file path
output_excel_file = "ecommerce_product_comparisons.xlsx"

# Write the DataFrame to an Excel file
try:
    filtered_df_results.write_excel(output_excel_file)
    print(f"\nComparison results successfully written to {output_excel_file}")
except Exception as e:
    print(f"Error writing comparison results to Excel: {e}")

# --- 6. UPLOAD RESULTS TO MINIO ---
if os.path.exists(output_excel_file):
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            endpoint_url=minio_endpoint,
            config=boto3.session.Config(signature_version='s3v4')
        )
        minio_bucket_name = 'data-pipeline-demo'
        minio_object_name = f'cache/{output_excel_file}'
        s3_client.upload_file(output_excel_file, minio_bucket_name, minio_object_name)
        print(f"Successfully uploaded {output_excel_file} to MinIO bucket '{minio_bucket_name}' as '{minio_object_name}'")
    except Exception as e:
        print(f"Error uploading {output_excel_file} to MinIO: {e}")
else:
    print(f"Excel file {output_excel_file} not found. Skipping MinIO upload.")

# --- 7. CLEANUP LOCAL FILES ---
print("\nCleaning up local files...")
# Remove the embeddings cache file
embedding_cache_file_to_remove = "embeddings_cache.pkl"
try:
    if os.path.exists(embedding_cache_file_to_remove):
        os.remove(embedding_cache_file_to_remove)
        print(f"Successfully removed local cache file: {embedding_cache_file_to_remove}")
    else:
        print(f"Local cache file not found, skipping removal: {embedding_cache_file_to_remove}")
except Exception as e:
    print(f"Error removing local cache file {embedding_cache_file_to_remove}: {e}")

# Remove the local Excel file (if it exists and after potential upload)
try:
    if os.path.exists(output_excel_file):
        os.remove(output_excel_file)
        print(f"Successfully removed local Excel file: {output_excel_file}")
    else:
        print(f"Local Excel file not found, skipping removal: {output_excel_file}")
except Exception as e:
    print(f"Error removing local Excel file {output_excel_file}: {e}")
