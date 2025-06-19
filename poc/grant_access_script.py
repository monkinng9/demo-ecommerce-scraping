from google.oauth2 import service_account
from googleapiclient.discovery import build

# Replace with the path to your service account key file
SERVICE_ACCOUNT_FILE = '/opt/airflow/private_key.json'
SCOPES = ['https://www.googleapis.com/auth/drive']

# Your personal Google account email address
YOUR_PERSONAL_EMAIL = 'beyondbegin41@gmail.com'

# The ID of the file you want to share
file_id = '1z0XmVVE_7HL8C9Wxtd_jfa0urC9NRpqH'

try:
    # Authenticate with the service account
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('drive', 'v3', credentials=creds)

    # Create the permission object for your personal email
    permission = {
        'type': 'user',
        'role': 'reader',  # Or 'writer' for edit access
        'emailAddress': YOUR_PERSONAL_EMAIL
    }

    # Add the permission to the file
    service.permissions().create(
        fileId=file_id,
        body=permission,
        sendNotificationEmail=False,  # Set to True to send an email notification
    ).execute()

    print(f"File '{file_id}' successfully shared with {YOUR_PERSONAL_EMAIL}")

except Exception as e:
    print(f"An error occurred: {e}")