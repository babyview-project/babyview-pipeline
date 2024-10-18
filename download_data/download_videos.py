import os
import io
import argparse
import pandas as pd

from datetime import datetime
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from moviepy.editor import VideoFileClip

import meta_extract.get_device_id as device
from meta_extract.get_highlight_flags import examine_mp4, sec2dtime
# all meta data types that we want to extract
ALL_METAS = [
    'ACCL', 'GYRO', 'SHUT', 'WBAL', 'WRGB', 'ISOE', 
    'UNIF', 'FACE', 'CORI', 'MSKP', 'IORI', 'GRAV', 
    'WNDM', 'MWET', 'AALP', 'LSKP'
    ]


class GoogleDriveDownloader:
    def __init__(self, args):
        self.args = args
        self.babyview_drive_id = '0AJtfZGZvxvfxUk9PVA'
        self.SCOPES = ['https://www.googleapis.com/auth/drive']
        self.total_video_count = 0
        self.video_durations = {}  # initialize an empty dictionary to keep track of video durations

    def load_existing_video_paths(self):
        if os.path.exists(self.args.csv_path):
            df = pd.read_csv(self.args.csv_path)
            return set(df['File Path'])
        return set()    

    def extract_meta(self, video_path, output_path):        
            for meta in ALL_METAS:
                meta_path = os.path.join(
                    output_path, f'{meta}_meta.txt')
                print(video_path, meta_path)
                cmd = f'../gpmf-parser/gpmf-parser {video_path} -f{meta} -a | tee {meta_path}'
                os.system(cmd)        
            self.get_highlight_and_device_id(video_path, output_path)

    def get_highlight_and_device_id(self, video_path, output_folder):
        def save_info(all_info, output_path, info_type):
            assert info_type in ['highlights', 'device_id'], \
                'info_type needs to be either device_id or highlights'
            str2insert = ""        
            str2insert += fname + "\n"
            if info_type == 'highlights':
                for i, highl in enumerate(all_info):
                    str2insert += "(" + str(i+1) + "): "
                    str2insert += sec2dtime(highl) + "\n"
            elif info_type == 'device_id':
                str2insert += all_info
            str2insert += "\n"
            with open(output_path, "w") as f:
                f.write(str2insert)

        fname = os.path.basename(video_path).split('.')[0]
        highlights = examine_mp4(video_path)
        highlights.sort()    
        highlight_path = os.path.join(output_folder, f'GP-Highlights_{fname}.txt')
        print(video_path)
        print(highlight_path)
        save_info(highlights, highlight_path, 'highlights')
        device_id = device.examine_mp4(video_path)    
        device_id_path = os.path.join(output_folder, f'GP-Device_name_{fname}.txt')
        save_info(device_id, device_id_path, 'device_id')
        print(device_id_path) 
        self.compress_vid(video_path, output_folder)    

    def compress_vid(self, video_path, output_folder):
        fname = os.path.basename(video_path).split('.')[0]
        output_path = os.path.join(output_folder, f'{fname}.mp4')
        cmd = f'ffmpeg -i {video_path} -vcodec libx264 -crf 28 {output_path}'
        os.system(cmd)

    def download_file(self, service, file_id, file_path):
        # do not download already existed file..
        if os.path.exists(file_path):
            return
        print(f"Downloading to: {file_path}")
        # add created date to file name
        create_date = service.files().get(
            fileId=file_id, 
            fields='createdTime', 
            supportsAllDrives=True
            ).execute()['createdTime']
        date_obj = datetime.strptime(create_date, "%Y-%m-%dT%H:%M:%S.%fZ")
        date_str = date_obj.strftime("%Y.%m.%d")
        directory, filename = os.path.split(file_path)
        name, extension = os.path.splitext(filename)
        file_name = f"{name}_{date_str}{extension}"
        file_path = os.path.join(directory, file_name)        
        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(file_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        breakpoint()
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}% complete.")        
        self.total_video_count += 1

    def get_video_duration(self, file_path):
        with VideoFileClip(file_path) as clip:
            duration = clip.duration            
            self.video_durations[file_path] = duration
            print(f"Video duration: {duration} seconds")

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
        # try:
        #     self.get_video_duration(file_path)
        # except Exception as e:
        #     print("Exception is:", e)
        
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
                fields="nextPageToken, files(id, name, mimeType, createdTime)",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                pageToken=page_token
            ).execute()
            items = results.get('files', [])
            for item in items:
                if item['mimeType'] == 'application/vnd.google-apps.folder':
                    self.recursive_search_and_download(service, item['id'], os.path.join(local_path, item['name']), existing_paths)
                elif item['name'].endswith('.MP4'):                                
                    self.download_and_get_duration(service, item['id'], os.path.join(local_path, item['name']), existing_paths)

            page_token = results.get('nextPageToken', None)
            if page_token is None:
                break

    def download_videos_from_drive(self):
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
    video_root = "/data2/ziyxiang/bv_tmp/raw/"
    output_folder = "/data2/ziyxiang/bv_tmp/processed/"
    cred_folder = "/ccn2/u/ziyxiang/cloud_credentials/babyview"
    parser = argparse.ArgumentParser(description="Download videos from cloud services")
    parser.add_argument('--video_root', type=str, default=video_root)
    parser.add_argument('--csv_path', type=str, default='video_durations.csv')
    parser.add_argument('--cred_folder', type=str, default=cred_folder)    
    parser.add_argument('--output_folder', type=str, default=output_folder)
    args = parser.parse_args()

    downloader = GoogleDriveDownloader(args)
    #downloader.get_existing_video_durations(args.video_root)
    downloader.download_videos_from_drive()    
    downloader.save_to_csv()
    downloader.print_video_stats()

if __name__ == '__main__':
    main()

