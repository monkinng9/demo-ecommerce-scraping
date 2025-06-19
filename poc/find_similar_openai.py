# Import required libraries
# ! pip install openpyxl openai polars numpy tqdm
import os
import polars as pl
import pickle
from openai import OpenAI
import numpy as np
from tqdm import tqdm
import concurrent.futures
import time

# --- Path Configuration ---
# Get the absolute path of the directory where the script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Construct the path to the 'data' directory (assuming it's one level up from SCRIPT_DIR, meaning 'data' is a sibling of the 'app' directory)
DATA_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..', 'data'))
# Ensure the data directory exists, create if not (especially for output files)
os.makedirs(DATA_DIR, exist_ok=True)

# --- 1. SETUP AND HELPER FUNCTIONS ---

# Initialize OpenAI client
# Ensure OPENAI_API_KEY environment variable is set
client = OpenAI()

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
embedding_cache_path = "recommendations_eucerin_embeddings_cache.pkl"

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
    """
    if string not in embedding_cache.keys():
        embedding_cache[string] = generate_embeddings(string)
        with open(embedding_cache_path, "wb") as embedding_cache_file:
            pickle.dump(embedding_cache, embedding_cache_file)
    return embedding_cache[string]

# --- 2. DATA LOADING AND CONTEXT PREPARATION ---

# Load the context dataset (our product catalog to search within)
context_dataset_path = os.path.join(DATA_DIR, "watson_eucerin_products.csv")
df_context = pl.read_csv(context_dataset_path)

# Load the query dataset (the products we want to find matches for)
query_dataset_path = os.path.join(DATA_DIR, "line_eucerin_products.csv")
df_query = pl.read_csv(query_dataset_path)

print("Preparing context by generating embeddings for all Watson's products...")
# Get all product names from the context dataframe
context_names = df_context.select('product_name').to_series().to_list()
# Generate and cache embeddings for all context product names
context_embeddings = [embedding_from_string(name) for name in context_names]
# Convert to a NumPy array for fast vector operations
context_embeddings_np = np.array(context_embeddings)
print(f"Context prepared. {len(context_embeddings_np)} product embeddings are loaded.\n")


# --- 3. CORE MATCHING LOGIC ---

def find_best_match_in_context(query_name, context_embeddings_np, top_n=3):
    """
    Finds the top N most similar products in the context catalog for a given query name.
    """
    query_embedding = embedding_from_string(query_name)
    query_embedding_np = np.array(query_embedding)

    # Efficient Cosine Similarity Calculation using NumPy
    dot_products = np.dot(context_embeddings_np, query_embedding_np)
    query_norm = np.linalg.norm(query_embedding_np)
    context_norms = np.linalg.norm(context_embeddings_np, axis=1)
    similarities = dot_products / (context_norms * query_norm)

    # Find the indices of the top N highest similarity scores
    best_match_indices = np.argsort(similarities)[-top_n:][::-1]

    return best_match_indices, similarities[best_match_indices]

def verify_products_similarity(base_product, product_list_to_compare):
    """
    Uses GPT-4o-mini to verify if any product in a list is the same as a base product.

    Args:
        base_product (str): The name of the base product.
        product_list_to_compare (list): A list of product names to compare against the base product.

    Returns:
        tuple: A tuple containing the matched product name and a boolean indicating if a match was found.
    """
    try:
        product_list_str = "\n".join([f"- {p}" for p in product_list_to_compare])
        prompt = f"Please verify if any of the products in the following list are the same as the base product. Base product: '{base_product}'. Comparison list:\n{product_list_str}\n\nRespond with the name of the product from the list that is the same, or 'False' if no product is the same."

        response = client.chat.completions.create(
            model="gpt-4.1-nano-2025-04-14",
            messages=[
                {"role": "system", "content": "You are an AI assistant that determines if products are the same based on their names. If a product from the list is the same as the base product, you must respond with the exact name of the product from the list. If no product is the same, you must respond with 'False'."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=50,
            temperature=0.0
        )

        content = response.choices[0].message.content.strip()
        
        if content.lower() == 'false':
            return None, False
        else:
            # Check if the response is one of the products in the list
            for product in product_list_to_compare:
                if content == product:
                    return product, True
            return None, False

    except Exception as e:
        print(f"An error occurred during gpt-4.1-nano-2025-04-14 verification: {e}")
        return None, False

# --- 4. PROCESS PRODUCTS AND CREATE DATAFRAME WITH COMPARISON RESULTS ---

def process_product_row(row):
    """
    Processes a single product row to find and verify matches with retry logic.

    This function attempts to find the best match for a given product from a
    context dataset. It performs the match and verification process up to 3
    times if an error occurs, with a 5-second delay between retries.

    Args:
        row (dict): A dictionary representing a row from the query dataframe.
                    It must contain 'product_name' and 'sale_price' keys.

    Returns:
        dict: A dictionary containing the comparison results for the product.
              If processing fails after all retries, it returns a dictionary
              with 'verified_match' set to False and placeholder values.
    """
    for attempt in range(3):
        try:
            query_product_name = row['product_name']
            query_product_price = row['sale_price']

            # Find best matches in the context
            match_indices, similarity_scores = find_best_match_in_context(query_product_name, context_embeddings_np)
            matched_products = [df_context.row(i, named=True) for i in match_indices]
            matched_product_names = [p['product_name'] for p in matched_products]

            # Verify the matches
            verified_product_name, is_match = verify_products_similarity(query_product_name, matched_product_names)

            if is_match:
                verified_product_index_in_matches = matched_product_names.index(verified_product_name)
                original_match_index = match_indices[verified_product_index_in_matches]
                similarity_score = similarity_scores[verified_product_index_in_matches]
                matched_product_info = df_context.row(original_match_index, named=True)
                
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
    future_to_row = {executor.submit(process_product_row, row): row for row in df_query.iter_rows(named=True)}
    
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
output_excel_file = os.path.join(DATA_DIR, "product_eucerin_comparisons.xlsx")

# Write the DataFrame to an Excel file
try:
    filtered_df_results.write_excel(output_excel_file)
    print(f"\nComparison results successfully written to {output_excel_file}")
except Exception as e:
    print(f"Error writing comparison results to Excel: {e}")
