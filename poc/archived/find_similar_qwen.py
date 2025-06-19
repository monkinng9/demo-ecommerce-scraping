# Import required libraries
import os
import polars as pl
import pickle
import dashscope
from dashscope import TextEmbedding
import numpy as np

# --- 1. SETUP AND HELPER FUNCTIONS ---

dashscope.base_http_api_url = 'https://dashscope-intl.aliyuncs.com/api/v1'
# Set DashScope API Key from environment variable
dashscope.api_key = os.getenv("DASHSCOPE_API_KEY")

# Define function: generate text embeddings
def generate_embeddings(text):
    """
    Generate embeddings for input text using DashScope's text-embedding-v3 model.
    """
    rsp = TextEmbedding.call(model=TextEmbedding.Models.text_embedding_v3, input=text)
    embeddings = [record['embedding'] for record in rsp.output['embeddings']]
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
context_dataset_path = "../data/watson_eucerin_products.csv"
df_context = pl.read_csv(context_dataset_path)

# Load the query dataset (the products we want to find matches for)
query_dataset_path = "../data/line_eucerin_products.csv"
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

def find_best_match_in_context(query_name, context_embeddings_np):
    """
    Finds the most similar product in the context catalog for a given query name.
    """
    query_embedding = embedding_from_string(query_name)
    query_embedding_np = np.array(query_embedding)
    
    # Efficient Cosine Similarity Calculation using NumPy
    dot_products = np.dot(context_embeddings_np, query_embedding_np)
    query_norm = np.linalg.norm(query_embedding_np)
    context_norms = np.linalg.norm(context_embeddings_np, axis=1)
    similarities = dot_products / (context_norms * query_norm)
    
    # Find the index of the highest similarity score
    best_match_index = np.argmax(similarities)
    
    return best_match_index, similarities[best_match_index]

# --- 4. CREATE DATAFRAME WITH COMPARISON RESULTS ---

# Create a list to hold the results
results_list = []

print("Finding matches and building DataFrame...")
# Iterate through each product in the query dataframe (line_products.csv)
for row in df_query.iter_rows(named=True):
    query_product_name = row['product_name']
    query_product_price = row['sale_price']

    # Use our function to find the best match from the Watson's context
    match_index, similarity_score = find_best_match_in_context(query_product_name, context_embeddings_np)
    
    # Get the details of the matched product from the context dataframe
    matched_product_info = df_context.row(match_index, named=True)
    matched_product_name = matched_product_info['product_name']
    matched_product_price = matched_product_info['sale_price']
    
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
filtered_df_results = df_results.filter(pl.col("similarity") >= 0.82)
filtered_df_results = filtered_df_results.with_columns([
    (pl.col('price_from_line') / pl.col('price_from_watson')).alias('diff')
])
filtered_df_results = filtered_df_results.filter(pl.col("diff") >= 1.5)

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
