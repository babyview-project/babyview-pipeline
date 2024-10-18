import os
import re
import shutil
import argparse
import logging
import pandas as pd

from datetime import datetime
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
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


# @TODO: uploaded meta data has overwriting issue due to the subject_id+video_id is not unique
# will need to fix them once the current uoload is done
# @TODO: update tracking sheet with the new video paths and date time
class GoogleDriveDownloader:
    def __init__(self, args):
        self.args = args
        " Babyview drive root IDs"
        self.babyview_drive_id = '0AJtfZGZvxvfxUk9PVA'
        self.storage_drive_id = '0AJGltX6vgytGUk9PVA'
        self.SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive']
        self.total_video_count = 0
        self.video_durations = {}  # initialize an empty dictionary to keep track of video durations
        self.datetime_tracking = self.sheet_to_dataframe()
        self.load_existing_video_paths()


    def load_existing_video_paths(self):                
        self.upload_videos_df = pd.read_csv(self.args.csv_path)
        self.upload_videos_df['File Path'] =  self.upload_videos_df['File Path'].apply(os.path.basename)
        self.original_videos_df = pd.read_csv(self.args.upload_csv_path)
        # use lambda to remove self.args.video_root from the file path
        self.original_videos_df['File Path'] = self.original_videos_df['File Path'].apply(lambda x: x.replace(self.args.video_root, ''))
        # use this to figure out the discrepancy between the tracking and the original videos caught by the pipeline
        self.original_videos_processed_list = self.original_videos_df['File Path'].values        
            

    # download tracking sheet as dataframe
    def sheet_to_dataframe(self):        
        # spreadsheet_id = '1mAti9dBNUqgNQQIIsnPb5Hu59ovKCUh9LSYOcQvzt2U'    # session tracking sheet        
        spreadsheet_id = '1mAti9dBNUqgNQQIIsnPb5Hu59ovKCUh9LSYOcQvzt2U'    # session tracking sheet
        # which sheet to download
        if self.args.bv_type == 'luna':
            range_name = 'Luna Videos'
        else:
            # @TODO: this is a temporary fix, for stuff we have done in the past
            range_name = 'Main_Release_1_Original'
                 
        service = self.build_google_drive_service(service_type='sheets')        
        # get the sheet info        
        sheet = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        values = sheet.get('values', [])        
        header = values[0] if values else []
        if self.args.bv_type == 'luna':
            padded_values = [row[:len(header)] for row in values]            
        else:
            padded_values = [row[:len(header)] for row in values[1:]]
        
        df = pd.DataFrame(padded_values, columns=header)         
        df['Original_Drive_Path'] = None
        df['Processed_Drive_Path'] = None
        return df
    
    """ Deal with inconsisitent date format between tracking sheet and the uploaded video file structure """
    def needs_expansion(self, date_range):        
        two_digit_year_pattern = r'\.\d{2}(?=[-|$])'
        
        if re.search(two_digit_year_pattern, date_range):
            return True
        return False
 

    def check_and_expand_date(self, date_range): 
        def full_year(match):
            return '20' + match.group(0)
        
        if self.needs_expansion(date_range):
            pattern = r'(?<=\.)\d{2}(?=-|$)'
            expanded_date_range = re.sub(pattern, full_year, date_range)
            return expanded_date_range
        
        return date_range


    def get_week_date_time_from_sheet(self, df, subject_id, video_id, record_period):        
        # fix inconsistent date format between tracking sheet and the uploaded video file structure
        record_period = self.check_and_expand_date(record_period)
        record_period = record_period.replace('.', '/')
        filtered_df = df[(df['subject_id'] == subject_id) & (df['video_id'] == video_id) & (df['Week'] == record_period)]
        # if there's no match, meaning the data is uploaded after the last tracking sheet update        
        if not filtered_df.empty:
            date = filtered_df.iloc[0]['Date']
            time = filtered_df.iloc[0]['Time']            
            return date, time
        else:                         
            return None, None


    def check_file(self, file_path):                
        directory, filename = os.path.split(file_path)
        video_id, extension = os.path.splitext(filename)
        fname_infos = os.path.dirname(
            os.path.relpath(file_path, self.args.video_root)).split('/')        
        subject_id = fname_infos[1]        

        if 'By_Date' in fname_infos:
            fname_infos.remove('By_Date')
        record_period = '.'.join(fname_infos[2:])            
        date, time = self.get_week_date_time_from_sheet(self.datetime_tracking, subject_id, video_id, record_period)        

        if subject_id == '00270001':
            video_id = video_id.split('_')[-1]        

        gdrive_file_path = file_path.replace(self.args.video_root, '')
        if date or time:
            print(f"Checking: {file_path}")
            datetime_str = f'{date}-{time}'.replace(' ', '').replace('/', '.')        
            file_name = f'{subject_id}_{video_id}_{record_period}_{datetime_str}{extension}'

            # add original drive path to the tracking sheet
            if gdrive_file_path in self.original_videos_df['File Path'].values:
                # when videos are not in the By_Date folder, it means they are not processed yet, so ignore them
                try:
                    useful_info_first, useful_info_second = gdrive_file_path.replace('/By_Date', '').split('-')
                    useful_info_first = useful_info_first.split('/')[1:]
                except Exception as e:
                    return

                useful_info_second = useful_info_second.split('/')
                subject_id = useful_info_first[0]
                video_id = useful_info_second[-1].split('.')[0]
                # again we deal with inconsistent date format between tracking sheet and the uploaded video file structure
                week = f"{'.'.join(useful_info_first[1:])}-{'.'.join(useful_info_second[:-1])}"
                week = self.check_and_expand_date(week).replace('.', '/')
                matching_row = self.datetime_tracking.loc[(self.datetime_tracking['subject_id'] == subject_id) & (self.datetime_tracking['video_id'] == video_id) & (self.datetime_tracking['Week'] == week)]                
                    
                # when there is no possibility for duplicate rows
                if len(matching_row) == 1:
                    self.datetime_tracking.loc[matching_row.index[0], "Original_Drive_Path"] = gdrive_file_path

                    # now check if the file is properly uploaded
                    if file_name in self.upload_videos_df['File Path'].values:
                        self.datetime_tracking.loc[matching_row.index[0], "Processed_Drive_Path"] = file_name
                elif len(matching_row) > 1:
                    # log the possibility of duplicated processed files
                    self.datetime_tracking.loc[matching_row.index[0], "Original_Drive_Path"] = gdrive_file_path
                    self.datetime_tracking.loc[matching_row.index[0], "Processed_Drive_Path"] = f"{file_name}/DUPLICATE"
            
            self.total_video_count += 1
            # save the tracking sheet every 20 videos, if exists, then append
            if self.total_video_count % 100 == 0:
                self.save_tracking_sheet()
                
                
    def save_tracking_sheet(self):    
        print(f"Checked {self.total_video_count} videos. Saving the tracking sheet...")    
        self.datetime_tracking.to_csv(self.args.date_time_tracking_sheet, index=False)
        self.total_video_count = 0


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


    def download_and_get_duration(self, file_path):
        relative_path = file_path.replace(self.args.video_root, '')                    
        self.check_file(file_path)


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
        # Specific folder to start with
        if self.args.bv_type == 'bing':
            entry_point_folder_id = "1-ATtN-wZ_mVY3Hm8Q0DO9CVizBsAmY6D"
            entry_point_folder_name = "BabyView_Bing"
        elif self.args.bv_type in ['main', 'luna']:
            entry_point_folder_id = "1ZfVyOBqb2L-Sw0b5himyg_ysB6Mwb8bo"
            entry_point_folder_name = "BabyView_Main"
        initial_local_path = os.path.join(self.args.video_root, entry_point_folder_name)
        self.recursive_search_and_download(service, entry_point_folder_id, initial_local_path)
        # save the tracking sheet
        self.save_tracking_sheet()        
        original_video_in_sheet = self.datetime_tracking['Original_Drive_Path'].values
        set_original_video_in_sheet = set(original_video_in_sheet)        
        

    def recursive_search_and_download(self, service, folder_id, local_path):        
        if not os.path.exists(local_path):
            if ' ' in local_path:
                local_path = local_path.replace(' ', '_')
            os.makedirs(local_path, exist_ok=True)
        page_token = None
        while True:
            query = f"'{folder_id}' in parents and trashed = false"
            results = service.files().list(
                driveId=self.babyview_drive_id,
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
                else:
                    if self.args.bv_type in ['main', 'bing']:
                        if item['name'].endswith('.MP4'):
                            self.download_and_get_duration(os.path.join(local_path, item['name']))
                    elif self.args.bv_type == 'luna':
                        if item['name'].endswith('.avi'):
                            self.download_and_get_duration(os.path.join(local_path, item['name']))

            page_token = results.get('nextPageToken', None)
            if page_token is None:
                break


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
        shutil.make_archive(zip_out_name, 'zip', root_dir=zip_folder)    
        video_path = os.path.join(zip_folder, [f for f in os.listdir(zip_folder) if f.endswith(".MP4")][0])
        return zipfile_path, video_path



def main():
    video_root = "/data2/ziyxiang/bv_tmp/raw/"
    output_folder = "/data2/ziyxiang/bv_tmp/processed/"
    cred_folder = "/ccn2/u/ziyxiang/cloud_credentials/babyview"
    parser = argparse.ArgumentParser(description="Download videos from cloud services")
    parser.add_argument('--bv_type', type=str, default='main', choices=['main', 'bing', 'luna'], help='Babyview Main or Bing')
    parser.add_argument('--video_root', type=str, default=video_root)
    parser.add_argument(
        '--csv_path', type=str, 
        default='/ccn2/u/ziyxiang/BabyView/duration/video_durations.csv',
        help='File where uploaded video and duration information is stored. This match what we have on the storage drive'
        )
    parser.add_argument(
        '--upload_csv_path', type=str, 
        default='/ccn2/u/ziyxiang/BabyView/uploaded_videos.csv',
        help='File where uploaded video information with original path is stored. This match what we have on the original drive.'
        )
    parser.add_argument('--date_time_tracking_sheet', type=str, default="./date_time_tracking_sheet.csv")
    parser.add_argument('--cred_folder', type=str, default=cred_folder)    
    parser.add_argument('--output_folder', type=str, default=output_folder)
    parser.add_argument('--error_log', type=str, default='error_log.txt')
    args = parser.parse_args()
    downloader = GoogleDriveDownloader(args)
    downloader.download_videos_from_drive()



if __name__ == '__main__':
    main()

