from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.decorators import task

# Ensure the file is named 'selenium_browser_proxy.py'
from extract_data.selenium_browser_proxy import get_watsons_token
from extract_data.watson_data_scraper import fetch_watsons_products
from extract_data.line_shopping_scraper import fetch_line_shopping_products
from data_processing.process_pre_landing_line_data import process_pre_landing_line_data
from data_processing.process_pre_landing_watson_data import process_pre_landing_watson_data
from data_processing.write_compare_price_report import create_comparison_report_and_upload


@task(task_id="get_and_update_token")
def get_token_and_update_variable():
    """Retrieves Watsons token and updates Airflow Variable."""
    print("Starting process to get Watsons token...")
    token = get_watsons_token()

    if token:
        print(f"Successfully retrieved token. Updating Airflow Variable 'WATSONS_BEARER_TOKEN'.")
        Variable.set("WATSONS_BEARER_TOKEN", token)
        print("Airflow Variable 'WATSONS_BEARER_TOKEN' has been updated.")
    else:
        print("Failed to retrieve Watsons token.")
        raise AirflowException("Could not retrieve the Watsons token after all retries.")


@task(task_id='scrape_watsons_data')
def run_watson_scraper():
    """Scrapes product data from Watsons."""
    search_queries = ["vistra", "blackmores", "dettol", "swisse", "eucerin"]
    for query in search_queries:
        print(f"\nFetching data for query: '{query}'")
        initial_data = fetch_watsons_products(query, page=0)
        if initial_data and 'pagination' in initial_data:
            total_pages = initial_data.get('pagination', {}).get('totalPages', 1)
            print(f"Found {initial_data.get('pagination', {}).get('totalResults', 0)} results across {total_pages} pages for '{query}'.")
            if total_pages > 1:
                for page_num in range(1, total_pages):
                    print(f"--- Fetching page {page_num + 1} of {total_pages} for '{query}' ---")
                    fetch_watsons_products(query, page=page_num)
        else:
            print(f"Failed to fetch initial data or pagination info for '{query}'.")


@task(task_id='scrape_line_shopping_data')
def run_line_shopping_scraper():
    """Scrapes product data from Line Shopping."""
    search_queries = ["vistra", "blackmores", "dettol", "swisse", "eucerin"]
    for query in search_queries:
        print(f"\nFetching Line Shopping data for query: '{query}'")
        initial_data = fetch_line_shopping_products(search_query=query, shops_page=1)
        if initial_data and initial_data.get('data') and initial_data['data'].get('shops'):
            total_pages = initial_data['data']['shops'].get('totalPage', 1)
            print(f"Found {initial_data['data']['shops'].get('totalShop', 0)} shops across {total_pages} pages for '{query}'.")
            if total_pages > 1:
                for page_num in range(2, total_pages + 1):
                    print(f"--- Fetching page {page_num} of {total_pages} for '{query}' ---")
                    fetch_line_shopping_products(search_query=query, shops_page=page_num)
        else:
            print(f"Failed to fetch initial data or shop info for '{query}'.")


@task(task_id='process_pre_landing_line_data')
def process_line_data_task():
    """Processes pre-landing Line Shopping data."""
    process_pre_landing_line_data()


@task(task_id='process_pre_landing_watson_data')
def process_watson_data_task():
    """Processes pre-landing Watson data."""
    process_pre_landing_watson_data()


@task(task_id='write_compare_price_report')
def write_compare_price_report_task():
    """Writes the price comparison report."""
    create_comparison_report_and_upload()


with DAG(
    dag_id="web_scraping_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,  # This makes it a manually triggered DAG
    tags=["extraction", "watsons", "line_shopping", "reporting"],
    doc_md="""
    ### E-commerce Product Price Comparison DAG

    This DAG performs the following steps:
    1.  **Get Watsons Token**: Retrieves a bearer token from watsons.co.th using Selenium and updates the `WATSONS_BEARER_TOKEN` Airflow Variable.
    2.  **Scrape Watsons Data**: Scrapes product data from Watsons based on a predefined list of search queries.
    3.  **Scrape Line Shopping Data**: Scrapes product data from Line Shopping for the same search queries.
    4.  **Process Data**: Processes the raw data from both sources.
    5.  **Generate Report**: Creates a price comparison report from the processed data.

    **Trigger:** Manual
    """
) as dag:
    get_and_update_token = get_token_and_update_variable()
    scrape_watsons = run_watson_scraper()
    scrape_line = run_line_shopping_scraper()
    process_watsons = process_watson_data_task()
    process_line = process_line_data_task()
    report_task = write_compare_price_report_task()

    get_and_update_token >> [scrape_watsons, scrape_line]
    scrape_watsons >> process_watsons
    scrape_line >> process_line
    [process_watsons, process_line] >> report_task