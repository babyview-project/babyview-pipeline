import os
import io
import argparse
import pandas as pd

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from moviepy.editor import VideoFileClip

#from concurrent.futures import ThreadPoolExecutor
import threading


class GoogleDriveDownloader:
    def __init__(self, args):
        self.args = args
        self.babyview_drive_id = '0AJtfZGZvxvfxUk9PVA'
        self.SCOPES = ['https://www.googleapis.com/auth/drive']
        self.total_video_count = 0
        self.video_durations = {}  # initialize an empty dictionary to keep track of video durations
        self.lock = threading.Lock()  # lock for thread-safety

    def load_existing_video_paths(self):
        if os.path.exists(self.args.csv_path):
            df = pd.read_csv(self.args.csv_path)
            return set(df['File Path'])
        return set()    

    def download_file(self, service, file_id, file_path):
        # @TODO: some files with the exact same name will be different, can ask
        # manual check to add a different name (-1, -2, -3)
        # do not download already existed file..
        if os.path.exists(file_path):
            return
        print(f"Downloading to: {file_path}")
        # Uncomment the below for actual download
        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(file_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}% complete.")
        with self.lock:
            self.total_video_count += 1

    def get_video_duration(self, file_path):
        with VideoFileClip(file_path) as clip:
            duration = clip.duration
            with self.lock:
                self.video_durations[file_path] = duration
                #print(f"Video duration: {duration} seconds")

    def download_and_get_duration(self, service, file_id, file_path, existing_paths):
        relative_path = file_path.replace(self.args.video_root, '')  # Get the relative path
        if relative_path in existing_paths:
            print(f"Skipping already existing video: {file_path}")
            return

        try:
            self.download_file(service, file_id, file_path)
        except Exception as e:
            print(f">>>>>>>>>>>>>>>>>>>>>> {file_path} failed to download..")
            print("Exception is", e)
        try:
            self.get_video_duration(file_path)
        except Exception as e:
            print("Exception is:", e)
        
    def get_existing_video_durations(self, root_path):
        for dirpath, _, filenames in os.walk(root_path):
            for file in filenames:
                if file.endswith('.MP4'):
                    file_path = os.path.join(dirpath, file)
                    try:
                        self.get_video_duration(file_path)
                    except Exception as e:
                        print(f"Error getting duration for {file_path}. Exception: {e}")
                        try:
                            os.remove(file_path)
                            print(f"Deleted {file_path} due to error.")
                        except Exception as delete_error:
                            print(f"Error deleting {file_path}. Exception: {delete_error}")

    #def recursive_search_and_download(self, service, folder_id, local_path):
    def recursive_search_and_download(self, service, folder_id, local_path, existing_paths):
        if not os.path.exists(local_path):
            os.makedirs(local_path)

        page_token = None
        while True:
            results = service.files().list(
                driveId=self.babyview_drive_id,
                corpora='drive',
                q=f"'{folder_id}' in parents and trashed = false",  # exclude trashed items
                pageSize=1000,
                fields="nextPageToken, files(id, name, mimeType)",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                pageToken=page_token
            ).execute()

            items = results.get('files', [])
            for item in items:
                if item['mimeType'] == 'application/vnd.google-apps.folder':
                    self.recursive_search_and_download(service, item['id'], os.path.join(local_path, item['name']), existing_paths)
                elif item['name'].endswith('.MP4'):
                    breakpoint()
                    self.download_and_get_duration(service, item['id'], os.path.join(local_path, item['name']), existing_paths)                    
            
            # with ThreadPoolExecutor(max_workers=self.args.max_workers) as executor:
            #     futures = []
            #     for item in items:
            #         if item['mimeType'] == 'application/vnd.google-apps.folder':
            #             self.recursive_search_and_download(service, item['id'], os.path.join(local_path, item['name']), existing_paths)
            #         elif item['name'].endswith('.MP4'):
            #             breakpoint()
            #             future = executor.submit(self.download_and_get_duration, service, item['id'], os.path.join(local_path, item['name']), existing_paths)
            #             futures.append(future)

            #     for future in futures:
            #         future.result()  # This will raise any exceptions encountered

            page_token = results.get('nextPageToken', None)
            if page_token is None:
                break

    def download_videos_from_drive(self):
        creds = None
        token_path = os.path.join(self.args.cred_folder, 'google_api_token.json')
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, self.SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                cred_path = os.path.join(self.args.cred_folder, 'credentials.json')
                flow = InstalledAppFlow.from_client_secrets_file(cred_path, self.SCOPES)
                creds = flow.run_local_server(port=self.args.port)
            with open(token_path, 'w') as token:
                token.write(creds.to_json())

        service = build('drive', 'v3', credentials=creds)                
        existing_paths = self.load_existing_video_paths()        
        # recursive search and download, skipping videos with paths already in the CSV
        self.recursive_search_and_download(service, self.babyview_drive_id, self.args.video_root, existing_paths)
        

    def save_to_csv(self):
        csv_path = self.args.csv_path
        # remove video_root prefix from file paths
        cleaned_paths = [(path.replace(self.args.video_root, ''), duration) for path, duration in self.video_durations.items()]
        new_data = pd.DataFrame(cleaned_paths, columns=['File Path', 'Duration (s)'])    
        # if CSV exists, append new data to it
        if os.path.exists(csv_path):
            existing_data = pd.read_csv(csv_path)
            combined_data = pd.concat([existing_data, new_data], ignore_index=True)
            combined_data.drop_duplicates(subset='File Path', keep='last', inplace=True)
            combined_data.to_csv(csv_path, index=False)
        else:
            new_data.to_csv(csv_path, index=False)

    
    def seconds_to_hms(self, seconds):
        """ Convert seconds to hh:mm:ss format
        """
        hours = seconds // 3600
        seconds %= 3600
        minutes = seconds // 60
        seconds %= 60
        return hours, minutes, seconds

    def print_video_stats(self):
        total_duration = sum(self.video_durations.values())
        total_videos = len(self.video_durations)
        hours, minutes, secs = self.seconds_to_hms(total_duration)

        print(f"Total Number of Videos: {total_videos}")
        print(f"Total Duration of Videos: {hours} hours {minutes} mins {secs:.2f} secs")

        

def main():
    video_root = "/data/ziyxiang/BabyView/"
    cred_folder = "/ccn2/u/ziyxiang/cloud_credentials/babyview"
    parser = argparse.ArgumentParser(description="Download videos from cloud services")
    parser.add_argument('--video_root', type=str, default=video_root)
    parser.add_argument('--csv_path', type=str, default='video_durations.csv')
    parser.add_argument('--cred_folder', type=str, default=cred_folder)
    #parser.add_argument('--max_workers', type=int, default=1)
    args = parser.parse_args()

    downloader = GoogleDriveDownloader(args)
    #downloader.get_existing_video_durations(args.video_root)
    downloader.download_videos_from_drive()    
    downloader.save_to_csv()
    downloader.print_video_stats()

if __name__ == '__main__':
    main()

