import os
import time
import tempfile
import shutil
from selenium import webdriver
from browsermobproxy import Server

def get_watsons_token():
    """
    Starts a proxy server and a headless Chrome browser to capture the authorization token from watsons.co.th.

    Returns:
        str: The captured authorization token, or None if not found.
    """
    proxy_path = "/opt/browsermob-proxy-2.1.4/bin/browsermob-proxy"
    if not os.path.exists(proxy_path):
        raise FileNotFoundError(f"BrowserMob Proxy not found at: {proxy_path}")

    server = Server(proxy_path, options={'port': 9092})
    server.start(options={'timeout': 15})

    authorization_token = None
    max_retries = 3
    final_driver_to_quit = None
    user_data_dir_for_cleanup = None

    try:
        current_run_user_data_dir = tempfile.mkdtemp()
        user_data_dir_for_cleanup = current_run_user_data_dir

        for attempt in range(max_retries):
            print(f"Attempt {attempt + 1} of {max_retries} to find Authorization token...")
            current_proxy_client = None
            current_driver = None

            try:
                current_proxy_client = server.create_proxy()
                chrome_options = webdriver.ChromeOptions()
                chrome_options.add_argument(f'--proxy-server={current_proxy_client.proxy}')
                chrome_options.add_argument('--ignore-certificate-errors')
                chrome_options.add_argument("--headless")
                chrome_options.add_argument("--no-sandbox")
                chrome_options.add_argument("--disable-dev-shm-usage")
                chrome_options.add_argument(f"--user-data-dir={current_run_user_data_dir}")
                chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
                chrome_options.add_argument("--window-size=1920,1080")

                current_driver = webdriver.Chrome(options=chrome_options)
                final_driver_to_quit = current_driver

                current_proxy_client.new_har("watsons.co.th", options={'captureHeaders': True, 'captureContent': True})
                current_driver.get("https://www.watsons.co.th")

                print(f"Waiting 10 seconds for page load and network traffic on attempt {attempt + 1}...")
                time.sleep(10)

                har_data = current_proxy_client.har
                for entry in har_data.get('log', {}).get('entries', []):
                    headers = entry.get('request', {}).get('headers', [])
                    for header in headers:
                        if header.get('name', '').lower() == 'authorization':
                            authorization_token = header.get('value')
                            break
                    if authorization_token:
                        break
                
                if authorization_token:
                    print(f"Authorization Token found on attempt {attempt + 1}.")
                    break
                else:
                    print(f"Authorization token not found after 10-second wait on attempt {attempt + 1}.")

            except Exception as e:
                print(f"An error occurred during attempt {attempt + 1}: {e}")
            
            finally:
                if not authorization_token:
                    if current_driver:
                        print(f"Quitting WebDriver for unsuccessful attempt {attempt + 1}.")
                        current_driver.quit()
                        if final_driver_to_quit == current_driver:
                            final_driver_to_quit = None

            if authorization_token:
                break
            elif attempt < max_retries - 1:
                print(f"Retrying after attempt {attempt + 1} failed...")
                time.sleep(10)

        if authorization_token:
            print(f"\nSuccessfully retrieved Authorization Token: {authorization_token}")
        else:
            print("\nFailed to retrieve Authorization token after all attempts.")
        
        return authorization_token

    finally:
        if final_driver_to_quit:
            print("Quitting the final WebDriver instance.")
            final_driver_to_quit.quit()
        
        if user_data_dir_for_cleanup and os.path.exists(user_data_dir_for_cleanup):
            print(f"Cleaning up user data directory: {user_data_dir_for_cleanup}")
            shutil.rmtree(user_data_dir_for_cleanup, ignore_errors=True)
        
        if 'server' in locals() and server:
            print("Stopping BrowserMob proxy server...")
            try:
                server.stop()
            except Exception as e:
                print(f"Error stopping server: {e}")

if __name__ == "__main__":
    token = get_watsons_token()
    if token:
        print(f"Final Token: {token}")
    else:
        print("Could not retrieve token.")
