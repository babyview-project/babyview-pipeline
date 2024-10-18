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



class VideoHighlights:
    """ Download processed zip files, and use information inside the meta data to get the highlight of the video. """
    def __init__(self, args):
        self.args = args
        self.storage_drive_id = '0AJGltX6vgytGUk9PVA'
        self.SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive']
        self.total_video_count = 0
        self.video_highlights = []
        

    def load_existing_video_paths(self):        
        if os.path.exists(self.args.csv_path):
            video_highlights_df = pd.read_csv(self.args.csv_path)            
            return set(video_highlights_df['File_Path'])
        
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


    def get_processed_highlights(self, service, file_id, file_path):        
        basename = os.path.basename(file_path)
        if not len(basename.split('_')) > 2:
            print(f"Issue with processed naming: {basename}")
            return
        relative_path = file_path.replace(self.args.video_root, '')  # Get the relative path        
        if relative_path in self.existing_paths:
            print(f"File {relative_path} already exists. Skipping...", flush=True)
            return
                        
        file_path, file_folder = self.download_file(service, file_id, file_path)

        if file_path:            
            zip_ref = zipfile.ZipFile(file_path, 'r')
            zip_ref.extractall(file_folder)
            for txt_file in os.listdir(file_folder):
                if txt_file.startswith('GP-Highlights'):
                    fullpath = os.path.join(file_folder, txt_file)
                    content = []
                    with open(fullpath, 'r') as file:
                        for line in file.readlines():
                            line = line.strip()
                            if line:
                                content.append(line)
                    
                    if len(content) == 1:                        
                        self.video_highlights.append([relative_path, 'No', 'NA'])
                    else:
                        breakpoint()
                        self.video_highlights.append([relative_path, 'Yes', '-'.join(content[1:])])

                    self.total_video_count += 1
                    break

            if self.total_video_count % 20 == 0 and self.video_highlights:
                # save video highlights to csv
                if not os.path.isfile(self.args.csv_path):
                    df = pd.DataFrame(self.video_highlights, columns=['File_Path', 'Highligh_Exist?', 'Highlight_Info'])
                    df.to_csv(self.args.csv_path, index=False)
                else:
                    # if the file already exists, append to it
                    df = pd.DataFrame(self.video_highlights, columns=['File_Path', 'Highligh_Exist?', 'Highlight_Info'])
                    df.to_csv(self.args.csv_path, mode='a', header=False, index=False)
                
                print(f"Saved {len(self.video_highlights)} video highlights to {self.args.csv_path}")
                self.video_highlights = []
                self.existing_paths = self.load_existing_video_paths()
                # removing everything from the directory to save local storage, try 3 times
                for _ in range(3):
                    try:
                        shutil.rmtree(file_folder)
                        return
                    except Exception as e:
                        continue

        

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
                elif item['name'].endswith('.ZIP') or item['name'].endswith('.zip'):
                    self.get_processed_highlights(service, item['id'], os.path.join(local_path, item['name']))                    

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
        # at the end of the download, save the remaining video highlights
        print(f"Total video count: {self.total_video_count}")
        if self.video_highlights:
            df = pd.DataFrame(self.video_highlights, columns=['File_Path', 'Highligh_Exist?', 'Highlight_Info'])
            df.to_csv(self.args.csv_path, mode='a', header=False, index=False)        
            print(f"Saved {len(self.video_highlights)} highlights to {self.args.csv_path}")



def main():
    parser = argparse.ArgumentParser(description="Data management pipeline for GoPro videos")
    # downloader args
    video_root = "./tmp"
    cred_folder = "/ccn2/u/ziyxiang/cloud_credentials/babyview"
    parser = argparse.ArgumentParser(description="Download videos from cloud services")
    parser.add_argument('--bv_type', type=str, default='main', choices=['main', 'bing'], help='Babyview Main or Bing')
    parser.add_argument('--video_root', type=str, default=video_root)
    parser.add_argument('--csv_path', type=str, default='video_highlights.csv')
    parser.add_argument('--cred_folder', type=str, default=cred_folder)        
    parser.add_argument('--error_log', type=str, default='error_log.txt')
    args = parser.parse_args()
    downloader = VideoHighlights(args)
    downloader.download_videos_from_drive()
    
    
if __name__ == '__main__':
    main()
