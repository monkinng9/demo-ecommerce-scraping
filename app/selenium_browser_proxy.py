# !pip install browsermob-proxy selenium
import os
import json

import time
import tempfile
import shutil
from selenium import webdriver
from browsermobproxy import Server

# --- Path Configuration ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Project root is one level up from the script's directory (app/)
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))


# Define the path to the browsermob-proxy executable
proxy_path = os.path.join(PROJECT_ROOT, 'browsermob-proxy-2.1.4', 'bin', 'browsermob-proxy')

if not os.path.exists(proxy_path):
    raise FileNotFoundError(f"BrowserMob Proxy not found at: {proxy_path}")

# Start the BrowserMob proxy server
server = Server(proxy_path, options={'port': 9092})
server.start(options={'timeout': 15})
authorization_token = None
max_retries = 3
final_driver_to_quit = None # Will hold the driver from a successful attempt or the last one if all fail
user_data_dir_for_cleanup = None # Variable to hold the path for cleanup

try:
    # Create a temporary directory for Chrome's user data for this run
    # This path will be used by all attempts in this script execution
    current_run_user_data_dir = tempfile.mkdtemp()
    user_data_dir_for_cleanup = current_run_user_data_dir # Ensure it's set for cleanup

    for attempt in range(max_retries):
        print(f"Attempt {attempt + 1} of {max_retries} to find Authorization token...")
        current_proxy_client = None
        current_driver = None # Driver for the current attempt

        try:
            # Create a new proxy client for this attempt
            current_proxy_client = server.create_proxy()

            # Configure Chrome WebDriver to use the new proxy
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument(f'--proxy-server={current_proxy_client.proxy}')
            chrome_options.add_argument('--ignore-certificate-errors')
            # The following options are helpful for running in a headless environment
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            # Add the unique user data directory argument
            chrome_options.add_argument(f"--user-data-dir={current_run_user_data_dir}")
            chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
            chrome_options.add_argument("--window-size=1920,1080")

            # Initialize the WebDriver for this attempt
            current_driver = webdriver.Chrome(options=chrome_options)
            final_driver_to_quit = current_driver # Tentatively set this as the driver to cleanup later

            # Start a new HAR capture
            current_proxy_client.new_har("watsons.co.th", options={'captureHeaders': True, 'captureContent': True})

            # Navigate to the target URL
            url = "https://www.watsons.co.th"
            current_driver.get(url)

            print(f"Waiting 10 seconds for page load and network traffic on attempt {attempt + 1}...")
            time.sleep(10)

            har_data = current_proxy_client.har
            current_attempt_token = None
            for entry in har_data.get('log', {}).get('entries', []):
                request_headers = entry.get('request', {}).get('headers', [])
                for header_info in request_headers: # Renamed 'header' to avoid conflict with a module
                    if header_info.get('name', '').lower() == 'authorization':
                        current_attempt_token = header_info.get('value')
                        break
                if current_attempt_token:
                    break
            
            if current_attempt_token:
                authorization_token = current_attempt_token
                print(f"Authorization Token found on attempt {attempt + 1}.")
                # Successful attempt, driver (`current_driver`) is stored in `final_driver_to_quit`
                break  # Exit the retry loop
            else:
                print(f"Authorization token not found after 10-second wait on attempt {attempt + 1}.")
                # This attempt failed, its driver will be cleaned up in this attempt's finally block
                # if authorization_token is still None.

        except Exception as e:
            print(f"An error occurred during attempt {attempt + 1}: {e}")
            # Error in this attempt, its driver will be cleaned up in this attempt's finally block
            # if authorization_token is still None.
        
        finally:
            # Cleanup for the current attempt if it FAILED or an error occurred
            if not authorization_token: # If token still not found (i.e., this attempt was not successful)
                if current_driver:
                    print(f"Quitting WebDriver for unsuccessful attempt {attempt + 1}.")
                    current_driver.quit()
                    if final_driver_to_quit == current_driver: # If this was the driver assigned
                        final_driver_to_quit = None # Nullify it as it's now quit
            # If authorization_token IS set, it means this attempt was successful.
            # We DO NOT quit current_driver here; it's already in final_driver_to_quit.
            
            # Proxy client cleanup is generally handled by server.stop() or when a new one is created.

        if authorization_token: # If token found, no need to continue to next attempt.
            break 
        elif attempt < max_retries - 1: # If token not found and not the last attempt
            print(f"Retrying after attempt {attempt + 1} failed...")
            time.sleep(10) # Optional: short delay before next retry
        # If it's the last attempt and it failed, loop terminates.

    # After the loop, report the final status
    if authorization_token:
        print(f"\nSuccessfully retrieved Authorization Token: {authorization_token}")
        os.environ["WATSONS_BEARER_TOKEN"] = authorization_token
        print("Set 'WATSONS_BEARER_TOKEN' in environment for current session.")

        # Save/update the authorization token in .env file
        dotenv_path = os.path.join(PROJECT_ROOT, '.env')

        lines = []
        if os.path.exists(dotenv_path):
            with open(dotenv_path, 'r') as f:
                lines = f.readlines()

        key_to_set = "WATSONS_BEARER_TOKEN"
        new_line = f'{key_to_set}="{authorization_token}"\n'
        key_found = False
        for i, line in enumerate(lines):
            if line.strip().startswith(f"{key_to_set}="):
                lines[i] = new_line
                key_found = True
                break
        
        if not key_found:
            lines.append(new_line)

        try:
            with open(dotenv_path, 'w') as f:
                f.writelines(lines)
            print(f"Updated 'WATSONS_BEARER_TOKEN' in {dotenv_path}")
        except IOError as e:
            print(f"Error writing to {dotenv_path}: {e}")
    else:
        print("\nFailed to retrieve Authorization token after all attempts.")

finally:
    # Overall cleanup
    if final_driver_to_quit:
        print("Quitting the final WebDriver instance.")
        final_driver_to_quit.quit()
    
    # Cleanup the temporary user data directory
    if user_data_dir_for_cleanup and os.path.exists(user_data_dir_for_cleanup):
        print(f"Cleaning up user data directory: {user_data_dir_for_cleanup}")
        shutil.rmtree(user_data_dir_for_cleanup, ignore_errors=True)
    
    if 'server' in locals() and server: # Check if server object exists and was assigned
        print("Stopping BrowserMob proxy server...")
        try:
            server.stop()
        except Exception as e:
            print(f"Error stopping server: {e}")
    else:
        print("BrowserMob proxy server was not started or 'server' object not found.")
