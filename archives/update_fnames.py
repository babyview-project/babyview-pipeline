import os
import time
import io
import shutil
import traceback
import argparse
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('./log.txt'),
        logging.StreamHandler()
    ]
)
import subprocess
import pandas as pd

from tqdm import tqdm
from datetime import datetime
from string import ascii_uppercase
from typing import List, Dict, Any
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

logging.basicConfig(
    filename='error_log.txt', filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.ERROR
)



class GoogleDriveDownloader:

    def __init__(self, args):
        self.args = args
        " Babyview drive root IDs"
        self.babyview_drive_id = '0AJtfZGZvxvfxUk9PVA'
        self.storage_drive_id = '0AJGltX6vgytGUk9PVA'        
        self.total_video_count = 0
        # keep track of the video durations
        self.video_durations = {}
        self._prep_services()
        self.datetime_tracking = self.sheet_to_dataframe()
        self.gcs_buckets = self.storage_client_instance.list_gcs_buckets()        
        logging.info(f"GCP_existing_buckets: {self.gcs_buckets}")


    def _prep_services(self):
        """ Prepare the Google Drive and Google Sheets services used through out the pipeline """
        self.SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive']
        self.drive_service = self.build_google_drive_service(service_type='drive')
        self.sheets_service = self.build_google_drive_service(service_type='sheets')


    def get_file_id_by_path(self, path_list: List[str]) -> str:        
        """ Takes a list of folder names and the file name then returns the file ID """        
        if self.args.bv_type == 'bing':
            folder_id = "1-ATtN-wZ_mVY3Hm8Q0DO9CVizBsAmY6D"            
        elif self.args.bv_type in ['main', 'luna', 'main_corrected', 'luna_corrected']:
            folder_id = "1ZfVyOBqb2L-Sw0b5himyg_ysB6Mwb8bo"            

        kwargs = dict(
            driveId=self.babyview_drive_id, 
            corpora='drive', 
            includeItemsFromAllDrives=True, 
            supportsAllDrives=True, 
            fields="files(id, name)"
            )
        for folder_name in path_list[:-1]:
            query = f"'{folder_id}' in parents and name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder'"
            results = self.drive_service.files().list(q=query, **kwargs).execute()
            items = results.get('files', [])
            if not items:
                print(f'Folder "{folder_name}" not found.')
                return None
            folder_id = items[0]['id']            
        
        file_name = path_list[-1]        
        query = f"'{folder_id}' in parents and name = '{file_name}'"
        results = self.drive_service.files().list(q=query, **kwargs).execute()
        items = results.get('files', [])
        if not items:
            print(f'File "{file_name}" not found.')
            return None

        return items[0]['id']


    def get_downloading_file_paths(self) -> Dict[str, str]:
        downloading_file_info = []
        # @TODO: Temporary selecting row ranges in different runs to process in parallel, 
        # with head, tail and         
        for idx, row in tqdm(self.datetime_tracking.iterrows()):
            if self.args.bv_type in ['main', 'luna', 'main_corrected', 'luna_corrected']:
                subject_id = row['subject_id']
                video_id = row['video_id']
                week = row['Week']                
                date = row['Date']
                # only process videos that have not been processed or have not been uploaded                
                # if (not processed_date or status != 'Uploaded') and is_subject_id and manual_process_date:
                if 'LUNA' in video_id:
                    video_name = f'{video_id}.avi'
                else:
                    if video_id.startswith('GX'):
                        video_name = f'{video_id}.MP4'
                    else:
                        video_name = f'{video_id}.LRV'
                
                folder_list = [subject_id, 'By Date', week, video_name]
                file_id = self.get_file_id_by_path(folder_list)
                week_str = week.replace('/', '.')
                file_path = f'{subject_id}/By Date/{week_str}/{video_name}'
                if file_id:
                        # on drive, the first content row starts at 2
                        downloading_file_info.append({
                            'idx': idx+2, 'file_id': file_id, 'file_path': file_path, 
                            # need to add these information to the dictionary
                            'Processed_date': '', 'Status': '', 'Duration': ''
                        })
                else:
                    logging.error(f'File ID not found for {file_path}')                    
                    downloading_file_info.append({
                        'idx': idx+2, 'file_id': file_id, 'file_path': file_path, 
                        # need to add these information to the dictionary
                        'Processed_date': '', 'Status': 'not found', 'Duration': ''
                    })

            else:    # special processing for bing
                subject_id = row['subject_id']
                video_id = row['video_id']
                processed_date = row['Processed_date']
                status = row['Status']
                date = row['Date']
                is_subject_id = True if self.args.subject_id == 'all' else subject_id == self.args.subject_id

                if True:
                    if video_id.startswith('GX'):
                        video_name = f'{video_id}.MP4'
                    else:
                        video_name = f'{video_id}.LRV'
                    
                    folder_list = [subject_id, date, video_name]
                    file_id = self.get_file_id_by_path(folder_list)
                    date_str = date.replace('/', '.')
                    file_path = f'{subject_id}/{date_str}/{video_name}'
                    if file_id:                        
                                # on drive, the first content row starts at 2
                                downloading_file_info.append({
                                    'idx': idx+2, 'file_id': file_id, 'file_path': file_path, 
                                    # need to add these information to the dictionary
                                    'Processed_date': '', 'Status': '', 'Duration': ''
                                })
                    else:
                        logging.error(f'File ID not found for {file_path}')
                        downloading_file_info.append({
                            'idx': idx+2, 'file_id': file_id, 'file_path': file_path, 
                            # need to add these information to the dictionary
                            'Processed_date': '', 'Status': 'not found', 'Duration': ''
                        })

        return downloading_file_info


    def extract_meta(self, video_path, output_path):
        extract_success = True
        for meta in ALL_METAS:
            meta_path = os.path.join(output_path, f'{meta}_meta.txt')
            print(video_path, meta_path)
    
        # no need to compress if meta data extraction fails (video corrupted)
        if extract_success:
            return self.get_highlight_and_device_id(video_path, output_path)
        else:
            return extract_success
        

    def get_highlight_and_device_id(self, video_path, output_folder):        
        fname = os.path.basename(video_path).split('.')[0]        
        print(video_path)        
        device_id_path = os.path.join(output_folder, f'GP-Device_name_{fname}.txt')
        print(device_id_path)
        return self.compress_vid(video_path, output_folder)


    def compress_vid(self, video_path, output_folder):
        fname = os.path.basename(video_path)       
        return fname  # signal success if compression succeeds


    # download tracking sheet as dataframe
    def sheet_to_dataframe(self):
        # THESE ARE THE HEADERS THAT ARE REQUIRED IN THE SHEET for the pipeline
        # self.required_headers = {'Processed_date', 'Status', 'Duration'}
        self.required_headers = {"Upload_fname"}
        self.spreadsheet_id = '1mAti9dBNUqgNQQIIsnPb5Hu59ovKCUh9LSYOcQvzt2U'  # session tracking sheet
        # which sheet to download
        if self.args.bv_type == 'luna':
            self.range_name = 'Luna_Round_2_Ongoing'
        elif self.args.bv_type == 'luna_corrected':
            self.range_name = 'Luna_V1_Corrected'            
        elif self.args.bv_type == 'main':
            self.range_name = 'Ongoing_data_collection'
        elif self.args.bv_type == 'main_corrected':
            self.range_name = 'Main_Release_1_Corrected'
        elif self.args.bv_type == 'bing':
            self.range_name = 'Bing'
                
        # get the sheet info 
        sheet = self.sheets_service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id, range=self.range_name
            ).execute()
        values = sheet.get('values', [])
        header = values[0] if values else []
        # pad the values with empty strings to make sure all rows have the same length        
        padded_values = [row + [''] * (len(header) - len(row)) for row in values[1:]]        
        # Create a pandas DataFrame from the padded values
        df = pd.DataFrame(padded_values, columns=header)        
        assert self.required_headers.issubset(df.columns), \
            f"Missing required headers: {self.required_headers - set(df.columns)}. Please add them to the sheet."
        return df
    

    def get_week_date_time_from_sheet(self, df, subject_id, video_id, week):
        # Filter the DataFrame for rows matching the subject_id and video_id
        filtered_df = df[(df['subject_id'] == subject_id) & (df['video_id'] == video_id) & (df['Week'] == week)]
        # Assuming there's only one match, or you want the first match
        if not filtered_df.empty:
            date = filtered_df.iloc[0]['Date']
            date = week.split('-')[0] if date == 'NA' else date            
            time = filtered_df.iloc[0]['Time']
            return date, time
        else:
            return None, None  # or raise an exception if you prefer


    def download_file(self, service, file_id, file_path):        
        directory, filename = os.path.split(file_path)
        video_id, extension = os.path.splitext(filename)
        fname_infos = os.path.dirname(os.path.relpath(file_path, self.args.video_root)).split('/')
        bv_main_folder = fname_infos[0]  # BabyView_Main, BabyView_Bing, BabyView_Play
        subject_id = fname_infos[1]
        record_period = fname_infos[-1]
        if 'By_Date' in fname_infos:
            fname_infos.remove('By_Date')
        if 'By Date' in fname_infos:
            fname_infos.remove('By Date')

        if self.args.bv_type in ['main', 'luna', 'main_corrected', 'luna_corrected']:
            # with '/' to match the format in the sheet
            week = record_period.replace('.', '/')        
            date, time = self.get_week_date_time_from_sheet(self.datetime_tracking, subject_id, video_id, week)        
            # add created date to file name
            if date is None or time is None:
                create_date = service.files().get(
                    fileId=file_id,
                    fields='createdTime',
                    supportsAllDrives=True
                ).execute()['createdTime']
                date_obj = datetime.strptime(create_date, "%Y-%m-%dT%H:%M:%S.%fZ")
                if date is None:
                    date = date_obj.strftime('%Y-%m-%d')
                if time is None:
                    time = date_obj.strftime('%H:%M:%S')

            datetime_str = f'{date}-{time}'.replace(' ', '').replace('/', '.')        
            file_name = f'{subject_id}_{video_id}_{record_period}_{datetime_str}{extension}'
        else:
            file_name = f'{subject_id}_{video_id}_{record_period}{extension}'
            
        os.makedirs(directory, exist_ok=True)
        raw_path = os.path.join(directory, file_name).replace(' ', '_')
        # folder to store processed video & meta data
        processed_folder = os.path.join(self.args.output_folder, bv_main_folder, subject_id, video_id)
        # if os.path.exists(raw_path):
        #     print(f"File already exists: {raw_path}")
        #     return raw_path, processed_folder
        
        # print(f"Downloading to: {file_path}")
        # if not os.path.exists(processed_folder):
        #     os.makedirs(processed_folder, exist_ok=True)
        # request = service.files().get_media(fileId=file_id)
        # fh = io.FileIO(raw_path, 'wb')
        # downloader = MediaIoBaseDownload(fh, request)
        # done = False
        # while not done:
        #     status, done = downloader.next_chunk()
        #     print(f"Download {int(status.progress() * 100)}% complete.")
        # self.total_video_count += 1
        return raw_path, processed_folder
    

    def clear_directory_contents(self, dir_path):
        """ Remove everything inside a directory path """
        if not os.path.isdir(dir_path):
            print("The specified directory does not exist.")
            return

        for filename in os.listdir(dir_path):
            file_path = os.path.join(dir_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.remove(file_path)

                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")


    def upload_file_gcp(self, gcp_bucket_name, zip_path, video_path, common_folder):
        # upload video file
        gcp_video_gcp_path = video_path.split(common_folder)[-1]        
        processed_vid_gcp_msg, processed_success = self.storage_client_instance.upload_file_to_gcs(
            source_file_name=video_path, destination_path=gcp_video_gcp_path, gcp_bucket=gcp_bucket_name
            )
        self.storage_client_instance.logs['processed_details'].append(processed_vid_gcp_msg)
        if processed_success:
            self.storage_client_instance.logs['processed_success'] += 1
        else:
            self.storage_client_instance.logs['processed_failure'] += 1

        # zip file
        gcp_zip_gcp_path = zip_path.split(common_folder)[-1]
        zip_gcp_msg, zip_success = self.storage_client_instance.upload_file_to_gcs(
            source_file_name=zip_path, destination_path=gcp_zip_gcp_path, gcp_bucket=gcp_bucket_name
            )
        self.storage_client_instance.logs['zip_details'].append(zip_gcp_msg)
        if zip_success:
            self.storage_client_instance.logs['zip_success'] += 1
        else:
            self.storage_client_instance.logs['zip_failure'] += 1


    def build_google_drive_service(self, service_type='drive'):
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
        version = 'v3' if service_type == 'drive' else 'v4'
        return build(service_type, version, credentials=creds)
        


    def download_videos_from_drive(self):        
        downloading_file_info  = self.get_downloading_file_paths()

        if self.args.bv_type == 'bing':            
            entry_point_folder_name = "BabyView_Bing"
        elif self.args.bv_type in ['main', 'luna', 'main_corrected', 'luna_corrected']:            
            entry_point_folder_name = "BabyView_Main"

        for i, video_info in enumerate(downloading_file_info):
            file_id = video_info['file_id']
            download_path = video_info['file_path']            
            
            if file_id:
                download_path = os.path.join(self.args.video_root, entry_point_folder_name, download_path)
                download_folder = os.path.dirname(download_path).replace('By Date', 'By_Date')
                os.makedirs(download_folder, exist_ok=True)
                try:
                    raw_path, processed_folder = self.download_file(self.drive_service, file_id, download_path)            
                except Exception as e:                
                    logging.info(f"Failed to download {file_id}...{e}")
                    video_info['Status'] = 'Download failed'
                    continue
            else:
                logging.info(f"File id not available for {file_id}")
                video_info['Status'] = 'Not found'
                raw_path = None

            if raw_path:                
                os.makedirs(processed_folder, exist_ok=True)
                # LUNA avi videos do not have meta data, will just compress, but GoPro videos have metadata
                if (self.args.bv_type in ['luna', 'luna_corrected'] and 'LUNA' in raw_path) or raw_path.endswith('LRV'):
                    video_ext = '.avi'
                    video_fname = self.compress_vid(raw_path, processed_folder)
                else:
                    video_ext = '.MP4'
                    try:
                        video_fname = self.extract_meta(raw_path, processed_folder)                        
                    except Exception as e:
                        logging.info(f">>>>>>>>>>>>>>>>>>>>>> {raw_path} failed to process..")
                        logging.info("Exception is", e)
                        video_fname = False                            
                        print(f"Process success {video_fname}...")                        

                if video_fname:
                    zip_output_path = os.path.join(os.path.dirname(processed_folder), video_fname)
                    zip_output_path = zip_output_path.replace(video_ext, '').split("/")[-1]
                    zip_path = f"{zip_output_path}.zip"
                    row_idx = video_info['idx']
                    columns = self.datetime_tracking.columns            
                    columns_str_idx_dict = {col: ascii_uppercase[idx] for idx, col in enumerate(columns)}
                    col_idx = columns_str_idx_dict['Upload_fname']
                    cell = f'{self.range_name}!{col_idx}{row_idx}'
                    if i and i % 60 == 0:
                        time.sleep(120)

                    self.sheets_service.spreadsheets().values().update(
                        spreadsheetId=self.spreadsheet_id, range=cell, valueInputOption='RAW',
                        body={'values': [[zip_path]]}
                    ).execute()
    

    def save_to_csv(self):
        csv_path = self.args.csv_path
        # remove video_root prefix from file paths
        cleaned_paths = [(path.replace(self.args.video_root, ''), duration) for path, duration in
                         self.video_durations.items()]
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

    def zip_files(self, zip_folder, zip_out_name):
        zipfile_path = f"{zip_out_name}.zip"
        print(f"Archive {zip_folder} to {zipfile_path}")
        video_path = os.path.join(zip_folder, [f for f in os.listdir(zip_folder) if f.endswith(".MP4")][0])
        return zipfile_path, video_path    


def main():
    video_root = "/data2/ziyxiang/bv_tmp/raw/"
    output_folder = "/data2/ziyxiang/bv_tmp/processed/"
    # cred_folder = "/ccn2/u/ziyxiang/cloud_credentials/babyview"    
    cred_folder = "creds"
    parser = argparse.ArgumentParser(description="Download videos from cloud services")
    parser.add_argument('--bv_type', type=str, default='main', choices=['main', 'bing', 'luna', 'luna_corrected', 'main_corrected'],
                        help='Babyview Main or Bing')
    # @TODO: temporarily to run multiple processes for each subject
    parser.add_argument('--subject_id', type=str, default='all', help='Subject ID to download videos for')
    parser.add_argument('--video_root', type=str, default=video_root)
    parser.add_argument('--csv_path', type=str, default='uploaded_videos.csv')
    parser.add_argument('--cred_folder', type=str, default=cred_folder)
    parser.add_argument('--output_folder', type=str, default=output_folder)
    parser.add_argument('--error_log', type=str, default='error_log.txt')    
    args = parser.parse_args()
    downloader = GoogleDriveDownloader(args)
    downloader.download_videos_from_drive()


if __name__ == '__main__':
    main()
