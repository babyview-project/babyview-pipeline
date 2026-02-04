

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import io
import os

# If modifying these SCOPES, delete the file google_api_token.json.
SCOPES = ['https://www.googleapis.com/auth/drive']
download_path = '/data/babyview/'
DRIVE_ID = '0AJGltX6vgytGUk9PVA'

def recursive_search_and_download(service, folder_id, local_path):        
    if not os.path.exists(local_path):
        if ' ' in local_path:
            local_path = local_path.replace(' ', '_')
        os.makedirs(local_path, exist_ok=True)
    page_token = None
    while True:
        query = f"'{folder_id}' in parents and trashed = false"
        results = service.files().list(
            driveId=DRIVE_ID,
            corpora='drive',
            q=query,
            pageSize=1000,
            fields="nextPageToken, files(id, name, mimeType, createdTime)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageToken=page_token
        ).execute()
        items = results.get('files', [])
        for item in items:
            if item['mimeType'] == 'application/vnd.google-apps.folder':
                recursive_search_and_download(service, item['id'], os.path.join(local_path, item['name']))
            elif item['name'].endswith('.MP4'):
                file_path = os.path.join(local_path, item['name'])
                if os.path.exists(file_path):
                    print("Skipping existing video...")
                    continue
                print(u'{0} ({1})'.format(item['name'], item['id']))
                request = service.files().get_media(fileId=item['id'])
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                    print("Download %d%%." % int(status.progress() * 100), end="\r")
                
                with open(file_path, 'wb') as f:
                    f.write(fh.getbuffer())

        page_token = results.get('nextPageToken', None)
        if page_token is None:
            break

def main():
    creds = None
    token_path = os.path.join('./google_api_token.json')
    # The file google_api_token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.path.exists('./google_api_token.json'):
        creds = Credentials.from_authorized_user_file('./google_api_token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('./credentials.json', SCOPES, redirect_uri='urn:ietf:wg:oauth:2.0:oob')
            auth_url, _ = flow.authorization_url(prompt='consent')
            print('Please go to this URL and authorize the app:')
            print(auth_url)
            code = input('Enter the authorization code: ')
            flow.fetch_token(code=code)
            creds = flow.credentials
        with open(token_path, 'w') as token:
            token.write(creds.to_json())
    # Call the Drive v3 API
    service = build('drive', 'v3', credentials=creds)  

    BING = False
    if BING:
        entry_point_folder_id = "1HizZZI5uWCvU647Uugc079hVHlv3IS9N"
        initial_local_path = os.path.join(download_path, "Babyview_Bing")
    else:
        entry_point_folder_id = "1-xadDZbpkA3n7b-UdOduocP5GfdeNIQd"
        initial_local_path = os.path.join(download_path, "Babyview_Main")
    recursive_search_and_download(service, entry_point_folder_id, initial_local_path)            

if __name__ == '__main__':
    main()
