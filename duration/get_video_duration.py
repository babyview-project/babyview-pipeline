import os
import io
import shutil
import zipfile
import argparse
import pandas as pd

from moviepy.editor import VideoFileClip
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow


ALL_METAS = [
    'ACCL', 'GYRO', 'SHUT', 'WBAL', 'WRGB',
    'ISOE', 'UNIF', 'FACE', 'CORI', 'MSKP',
    'IORI', 'GRAV', 'WNDM', 'MWET', 'AALP',
    'LSKP']



class VideoDuration:
    """ Download processed zip files, and use information inside the meta data to get the duration of the video. """
    def __init__(self, args):
        self.args = args
        self.storage_drive_id = '0AJGltX6vgytGUk9PVA'
        self.SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive']
        self.total_video_count = 0        
        self.video_durations = []
        self.load_duplicate_file_paths()

    def load_duplicate_file_paths(self):
        # load the duplicate file paths
        duplicate_file_path = 'new_duplicate_txt_files.csv'
        duplicate_df = pd.read_csv(duplicate_file_path)
        self.duplicate_file_names = [f.replace('.txt', '.MP4') for f in duplicate_df['File2'].values]
        

    def load_existing_video_paths(self):        
        if os.path.exists(self.args.csv_path):
            video_duration_df = pd.read_csv(self.args.csv_path)            
            return set(video_duration_df['File Path'])
        
        return set()


    def download_file(self, service, file_id, file_path):
        # do not download already existed file..
        if os.path.exists(file_path):
            return None, None
        
        print(f"Downloading to: {file_path}")        
        directory = os.path.dirname(file_path)                
        os.makedirs(directory, exist_ok=True)                
        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(file_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)        
        done = False

        while not done:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}% complete.")        
                
        return file_path, directory


    def extract_video_info(self, filename):
        with open(filename, 'r') as file:
            lines = file.readlines()
        
        framerate = 0
        total_frames = 0
        for i, line in enumerate(lines):
            if "VIDEO FRAMERATE:" in line:                
                next_line = lines[i+1].strip()
                parts = next_line.split()
                framerate = float(parts[0])
                total_frames = int(parts[-2])
                break
        
        if framerate != 0:
            duration_seconds = round(total_frames / framerate, 2)
            return duration_seconds
        else:
            return None


    def get_processed_duration(self, service, file_id, file_path):
        relative_path = file_path.replace(self.args.video_root, '')  # Get the relative path        
        if relative_path in self.existing_paths:
            print(f"File {relative_path} already exists. Skipping...", flush=True)
            return
                        
        file_path, file_folder = self.download_file(service, file_id, file_path)
        if file_path:        
            video = VideoFileClip(file_path)
            duration = video.duration
            video.close()
            size_bytes = os.path.getsize(file_path)
            size_mb = size_bytes / (1024 * 1024)
            self.video_durations.append([relative_path, duration, size_mb])
            self.total_video_count += 1

            if self.total_video_count % 20 == 0 and self.video_durations:
                # save video durations to csv
                if not os.path.isfile(self.args.csv_path):
                    df = pd.DataFrame(self.video_durations, columns=['File Path', 'Duration', 'File Size (MB)'])
                    df.to_csv(self.args.csv_path, index=False)
                else:
                    # if the file already exists, append to it
                    df = pd.DataFrame(self.video_durations, columns=['File Path', 'Duration', 'File Size (MB)'])
                    df.to_csv(self.args.csv_path, mode='a', header=False, index=False)
                
                print(f"Saved {len(self.video_durations)} video durations to {self.args.csv_path}")
                self.video_durations = []
                self.existing_paths = self.load_existing_video_paths()
                # removing everything from the directory to save local storage
                shutil.rmtree(file_folder)
        

    def recursive_search_and_download(self, service, folder_id, local_path):        
        if not os.path.exists(local_path):
            if ' ' in local_path:
                local_path = local_path.replace(' ', '_')
            os.makedirs(local_path, exist_ok=True)
        page_token = None
        while True:
            query = f"'{folder_id}' in parents and trashed = false"
            results = service.files().list(
                driveId=self.storage_drive_id,
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
                    self.recursive_search_and_download(service, item['id'], os.path.join(local_path, item['name']))                                             
                elif item['name'].endswith('.MP4') or item['name'].endswith('.mp4'):
                    self.get_processed_duration(service, item['id'], os.path.join(local_path, item['name']))                    

            page_token = results.get('nextPageToken', None)
            if page_token is None:
                break


    def build_google_drive_service(self, service_type='drive'):
        creds = None
        token_path = os.path.join(self.args.cred_folder, 'token.json')
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
        version = 'v3' if service_type == 'drive' else 'v4'        
        service = build(service_type, version, credentials=creds)
        return service


    def download_videos_from_drive(self):
        service = self.build_google_drive_service()        
        self.existing_paths = self.load_existing_video_paths()
        # Specific folder to start with
        if self.args.bv_type == 'bing':            
            entry_point_folder_id = "1HizZZI5uWCvU647Uugc079hVHlv3IS9N"
            entry_point_folder_name = "BabyView_Bing"
        elif self.args.bv_type == 'main':
            entry_point_folder_id = "1-xadDZbpkA3n7b-UdOduocP5GfdeNIQd"
            entry_point_folder_name = "BabyView_Main"

        initial_local_path = os.path.join(self.args.video_root, entry_point_folder_name)
        self.recursive_search_and_download(service, entry_point_folder_id, initial_local_path)
        # at the end of the download, save the remaining video durations
        print(f"Total video count: {self.total_video_count}")
        if self.video_durations:
            df = pd.DataFrame(self.video_durations, columns=['File Path', 'Duration', 'File Size'])
            df.to_csv(self.args.csv_path, mode='a', header=False, index=False)        
            print(f"Saved {len(self.video_durations)} video durations to {self.args.csv_path}")



def main():
    parser = argparse.ArgumentParser(description="Data management pipeline for GoPro videos")
    # downloader args
    video_root = "./tmp"
    cred_folder = "/ccn2/u/ziyxiang/cloud_credentials/babyview"
    parser = argparse.ArgumentParser(description="Download videos from cloud services")
    parser.add_argument('--bv_type', type=str, default='main', choices=['main', 'bing'], help='Babyview Main or Bing')
    parser.add_argument('--video_root', type=str, default=video_root)
    parser.add_argument('--csv_path', type=str, default='video_durations.csv')
    parser.add_argument('--cred_folder', type=str, default=cred_folder)        
    parser.add_argument('--error_log', type=str, default='error_log.txt')
    args = parser.parse_args()
    downloader = VideoDuration(args)
    downloader.download_videos_from_drive()
    
    
if __name__ == '__main__':
    main()
