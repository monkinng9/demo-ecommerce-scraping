# Import required libraries
import os
from typing import List, Optional, Dict
import polars as pl
import pickle
import google.generativeai as genai
import numpy as np
import time

# --- 1. SETUP AND HELPER FUNCTIONS ---

# Initialize Google Generative AI
# Ensure GOOGLE_API_KEY environment variable is set
# (As per project memory and user rules for storing API keys)
genai.configure(api_key=os.environ["GOOGLE_API_KEY"])

# Define function: generate text embeddings in batches with caching
def get_embeddings_with_caching(
    texts: List[str],
    cache: Dict[str, List[float]],
    cache_path: str,
    batch_size: int = 100  # Gemini API has a limit of 100 texts per call
) -> Dict[str, List[float]]:
    """
    Computes embeddings for a list of texts, using a cache to avoid re-computation.
    New embeddings are generated in batches to be efficient and avoid rate limits.

    Args:
        texts (List[str]): A list of strings to embed.
        cache (Dict[str, List[float]]): The dictionary acting as the cache.
        cache_path (str): Path to the pickle file for the cache.
        batch_size (int): The number of texts to embed in a single API call.

    Returns:
        Dict[str, List[float]]: The updated cache dictionary.
    """
    # Find which texts are not in the cache and need to be embedded
    texts_to_embed = sorted(list(set(t for t in texts if t.strip() and t not in cache)))

    if not texts_to_embed:
        print("All required embeddings were found in the cache.")
        return cache

    print(f"Generating embeddings for {len(texts_to_embed)} new items...")
    
    cache_updated = False
    # Process texts in batches
    for i in range(0, len(texts_to_embed), batch_size):
        batch = texts_to_embed[i:i + batch_size]
        try:
            print(f"  - Processing batch {i//batch_size + 1}/{ -(-len(texts_to_embed) // batch_size)}...")
            # Gemini API batch embedding call
            response = genai.embed_content(
                model="models/gemini-embedding-exp-03-07",
                content=batch,
                task_type="RETRIEVAL_DOCUMENT"
            )
            new_embeddings = response['embedding']
            
            # Add new embeddings to the cache
            for text, embedding in zip(batch, new_embeddings):
                cache[text] = embedding
            cache_updated = True

        except Exception as e:
            # If a batch fails, print an error and mark its items as failed in cache
            print(f"!! Error embedding batch starting with '{batch[0][:50]}...': {e}")
            print("!! This batch will be skipped. Storing empty embeddings in cache to prevent retries.")
            for text in batch:
                cache[text] = [] # Mark as failed
            cache_updated = True
        
        # A small delay between batches can help with very strict rate limits
        if len(texts_to_embed) > batch_size:
             time.sleep(1)

    if cache_updated:
        print("Saving updated embedding cache to file...")
        with open(cache_path, "wb") as f:
            pickle.dump(cache, f)

    return cache


# Set embedding cache path
embedding_cache_path = "gemini_eucerin_embeddings_cache.pkl"

# Try to load cached embeddings
try:
    with open(embedding_cache_path, 'rb') as f:
        embedding_cache = pickle.load(f)
except FileNotFoundError:
    embedding_cache = {}


# --- 2. DATA LOADING AND EMBEDDING PREPARATION ---

# Load the context dataset (our product catalog to search within)
context_dataset_path = "../data/watson_eucerin_products.csv"
df_context_original = pl.read_csv(context_dataset_path)

# Load the query dataset (the products we want to find matches for)
query_dataset_path = "../data/line_eucerin_products.csv"
df_query = pl.read_csv(query_dataset_path)

# Get all unique product names from both sources to embed them all at once efficiently
context_product_names = df_context_original.select('product_name').to_series().to_list()
query_product_names = df_query.select('product_name').to_series().to_list()
all_unique_product_names = list(set(context_product_names + query_product_names))

print(f"Found {len(all_unique_product_names)} unique product names across both files.")

# Update the cache for all unique products in a single, batched process
embedding_cache = get_embeddings_with_caching(all_unique_product_names, embedding_cache, embedding_cache_path)


print("\nPreparing context vectors from cache...")
raw_context_embeddings = []
valid_original_indices = [] # To keep track of which original products are kept

for i, name in enumerate(context_product_names):
    # Retrieve the pre-computed embedding from the cache
    embedding = embedding_cache.get(name, []) # Use .get with a default empty list
    if isinstance(embedding, (list, np.ndarray)) and len(embedding) > 0 and all(isinstance(x, (float, int)) for x in embedding):
        raw_context_embeddings.append(embedding)
        valid_original_indices.append(i)
    else:
        print(f"Warning: Skipping context product '{name}' (original index {i}) due to invalid/empty embedding in cache.")

if not raw_context_embeddings:
    print("Error: No validly formatted embeddings could be generated for any context products. Exiting.")
    exit()

# Convert to NumPy array
context_embeddings_np_temp = np.array(raw_context_embeddings, dtype=np.float32)

# Calculate norms
context_norms_temp = np.linalg.norm(context_embeddings_np_temp, axis=1)

# Identify indices (within raw_context_embeddings/valid_original_indices) where norm is non-zero
non_zero_norm_indices = np.where(context_norms_temp > 1e-9)[0]

if non_zero_norm_indices.size == 0:
    print("Error: All generated context embeddings have zero norm. Cannot proceed. Exiting.")
    exit()

# Filter everything based on non-zero norms
final_context_embeddings_np = context_embeddings_np_temp[non_zero_norm_indices]
final_context_norms = context_norms_temp[non_zero_norm_indices]

# Filter the original_indices list to get the final list of original indices that are kept
final_valid_original_indices = [valid_original_indices[i] for i in non_zero_norm_indices]

# Create the final df_context that corresponds to the final_context_embeddings_np
df_context_final_matchable = df_context_original[final_valid_original_indices]

print(f"Context prepared. {len(final_context_embeddings_np)} product embeddings are loaded and validated (non-zero norm).\n")


# --- 3. CORE MATCHING LOGIC ---

def find_best_match_in_context(query_embedding: List[float], context_embeddings_matchable: np.ndarray, context_norms_matchable: np.ndarray):
    """
    Finds the most similar product in the context catalog for a given query embedding.
    This function now expects a pre-computed embedding vector, not a string.

    Args:
        query_embedding (List[float]): The embedding vector of the query product.
        context_embeddings_matchable (np.ndarray): NumPy array of valid, non-zero-norm context embeddings.
        context_norms_matchable (np.ndarray): NumPy array of norms for context_embeddings_matchable.

    Returns:
        tuple: (best_match_index, similarity_score). 
               best_match_index is -1 if query embedding is invalid or has zero norm.
    """
    # The check for valid embedding now happens before calling this function.
    # We still perform a safety check here.
    if not isinstance(query_embedding, (list, np.ndarray)) or len(query_embedding) == 0:
        return -1, 0.0

    query_embedding_np = np.array(query_embedding, dtype=np.float32)
    query_norm = np.linalg.norm(query_embedding_np)

    if query_norm < 1e-9:
        # This case is unlikely if the embedding is valid, but it's a good safeguard.
        return -1, 0.0
    
    # Calculate cosine similarity
    dot_products = np.dot(context_embeddings_matchable, query_embedding_np)
    similarities = dot_products / (context_norms_matchable * query_norm)
    
    best_match_index = np.argmax(similarities)
    
    return best_match_index, similarities[best_match_index]

# --- 4. CREATE DATAFRAME WITH COMPARISON RESULTS ---

results_list = []

print("Finding matches and building DataFrame...")
# Iterate through each product in the query dataframe (line_products.csv)
for row in df_query.iter_rows(named=True):
    query_product_name = row['product_name']
    query_product_price = row['sale_price']

    # Get the pre-computed embedding from the cache
    query_embedding = embedding_cache.get(query_product_name, [])

    if query_embedding:
        # Use our function with the pre-computed embedding
        match_index_in_final_df, similarity_score = find_best_match_in_context(
            query_embedding, final_context_embeddings_np, final_context_norms
        )
    else:
        # This handles cases where the query embedding failed during the batch process
        print(f"Warning: Could not find a valid embedding for query '{query_product_name}'. Skipping.")
        match_index_in_final_df = -1
        similarity_score = 0.0

    if match_index_in_final_df != -1:
        # Get the details of the matched product from the filtered context dataframe
        matched_product_info = df_context_final_matchable.row(match_index_in_final_df, named=True)
        matched_product_name = matched_product_info['product_name']
        matched_product_price = matched_product_info['sale_price']
    else:
        # Handle cases where query embedding failed or no match could be computed
        matched_product_name = "N/A (Query Embedding Error)"
        matched_product_price = np.nan
        # similarity_score is already 0.0
    
    # Append the results as a dictionary to our list
    results_list.append({
        'base_product_name (from line)': query_product_name,
        'base_product_name (from watson)': matched_product_name,
        'price_from_line': query_product_price,
        'price_from_watson': matched_product_price,
        'similarity': similarity_score
    })

# Convert the list of results into a polars DataFrame
df_results = pl.DataFrame(results_list)
filtered_df_results = df_results.filter(pl.col("similarity") >= 0.8)
filtered_df_results = filtered_df_results.with_columns([
    (pl.col('price_from_line') / pl.col('price_from_watson')).alias('diff')
])
filtered_df_results = filtered_df_results.filter((pl.col("diff") < 1.5) & (pl.col("diff") > 0.5))

# Display the final DataFrame
print("\n--- Product Comparison DataFrame ---")
print(filtered_df_results)

# --- 5. SAVE RESULTS TO CSV ---

# Define the output CSV file path
output_csv_file = '../data/product_eucerin_comparisons.csv'

# Write the DataFrame to a CSV file
try:
    filtered_df_results.write_csv(output_csv_file)
    print(f"\nComparison results successfully written to {output_csv_file}")
except Exception as e:
    print(f"Error writing comparison results to CSV: {e}")