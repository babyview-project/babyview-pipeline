import os
import io
import re
import shutil
import traceback
import argparse
import logging

import settings
import util

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
from moviepy import VideoFileClip

import meta_extract.get_device_id as device
from meta_extract.get_highlight_flags import examine_mp4, sec2dtime

from gcp_storage_services import GCPStorageServices

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

storage_client_instance = GCPStorageServices()


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
        self.gcs_buckets = storage_client_instance.list_gcs_buckets()
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
        elif self.args.bv_type in ['main', 'luna']:
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

    def get_downloading_file_paths(self) -> list:
        downloading_file_info = []
        # @TODO: Temporary selecting row ranges in different runs to process in parallel, 
        # with head, tail and 
        for idx, row in tqdm(self.datetime_tracking.iterrows()):  #.iloc[1063: 2000].iterrows()):
            subject_id = row['subject_id']
            video_id = row['video_id']
            processed_date = row['Processed_date']
            status = row['Status']
            date = row['Date']
            gcp_name = row['new_name']
            file_id = None
            file_path = None
            session_num = video_id[3] if len(video_id) > 4 else 'NA'
            uniq_id = util.generate_unique_id()

            is_subject_id = True if self.args.subject_id == 'all' else subject_id == self.args.subject_id

            if self.args.bv_type in ['main', 'luna']:
                week = row['Week']
                time = row['Time']
                # this is the date when the RAs manually processed the video, which can be processed by the pipeline
                manual_process_date = row['date_processed']

                # only process videos that have not been processed or have not been uploaded
                if (not processed_date or status != 'Uploaded') and is_subject_id and manual_process_date:
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
                else:
                    logging.info(f'File {video_id} for {subject_id} cannot be processed at this time.')
            else:  # special processing for bing
                if (not processed_date or status != 'Uploaded') and is_subject_id:
                    if video_id.startswith('GX'):
                        video_name = f'{video_id}.MP4'
                    else:
                        video_name = f'{video_id}.LRV'

                    folder_list = [subject_id, date, video_name]
                    file_id = self.get_file_id_by_path(folder_list)
                    date_str = date.replace('/', '.')
                    file_path = f'{subject_id}/{date_str}/{video_name}'

            try:
                # Attempt to parse the date
                date_obj = datetime.strptime(date, "%m/%d/%Y")
                date_formatted = date_obj.strftime("%Y-%m-%d")
            except ValueError:
                date_formatted = "NA"
                print(f"Invalid date input for {file_id}, {subject_id}_{video_id}: {date}.")

            if file_id:
                # on drive, the first content row starts at 2
                downloading_file_info.append({
                    'idx': idx + 2, 'file_id': file_id, 'file_path': file_path,
                    'subject_id': subject_id, 'date': date_formatted, 'session_num': session_num,
                    'uniq_id': uniq_id, 'gcp_name': gcp_name, 'new_location_raw': '',
                    'new_location_storage_zip': '', 'new_location_storage_mp4': '',
                    # need to add these information to the dictionary
                    'Processed_date': '', 'Status': '', 'Duration': ''
                })
            else:
                logging.error(f'File ID not found for {file_path}')
                downloading_file_info.append({
                    'idx': idx + 2, 'file_id': file_id, 'file_path': file_path,
                    'subject_id': subject_id, 'date': date_formatted, 'session_num': session_num,
                    'uniq_id': uniq_id, 'gcp_name': gcp_name,'new_location_raw': '',
                    'new_location_storage_zip': '', 'new_location_storage_mp4': '',
                    # need to add these information to the dictionary
                    'Processed_date': '', 'Status': 'not found', 'Duration': ''
                })

        return downloading_file_info

    def extract_meta(self, video_path, output_path):
        extract_success = True
        for meta in ALL_METAS:
            meta_path = os.path.join(output_path, f'{meta}_meta.txt')
            print(video_path, meta_path)
            cmd = f'../gpmf-parser/gpmf-parser {video_path} -f{meta} -a | tee {meta_path}'

            try:
                result = subprocess.run(cmd, shell=True, check=True, capture_output=True, timeout=120)
                try:
                    output_text = result.stdout.decode('utf-8')
                except UnicodeDecodeError:
                    output_text = result.stdout.decode('utf-8', 'replace')  # Replace or ignore invalid characters
                print(output_text)
                if 'error' in output_text.lower():
                    logging.error(f'Error executing command: {cmd}\nError message: {output_text}')
                    extract_success = False
                    break
            # something is wrong with the video file
            except subprocess.CalledProcessError as e:
                # print(f"Inside extract_meta: {e.stderr}")
                logging.error(f'Error executing command: {cmd}\nError message: {e.stderr}')
                # signal failure if any of the meta data extraction fails
                extract_success = False
                break
            except subprocess.TimeoutExpired:
                logging.error(f"Command timed out: {cmd}")
                extract_success = False
                break
            except Exception as e:
                logging.error(f'Unexpected error while executing {cmd}: {traceback.format_exc()}')
                extract_success = False
                break
        # no need to compress if meta data extraction fails (video corrupted)
        if extract_success:
            return self.get_highlight_and_device_id(video_path, output_path)
        else:
            return extract_success

    def get_highlight_and_device_id(self, video_path, output_folder):
        def save_info(all_info, output_path, info_type):
            assert info_type in ['highlights', 'device_id'], \
                'info_type needs to be either device_id or highlights'
            str2insert = ""
            str2insert += fname + "\n"
            if info_type == 'highlights':
                for i, highl in enumerate(all_info):
                    str2insert += "(" + str(i + 1) + "): "
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
        return self.compress_vid(video_path, output_folder)

    def compress_vid(self, video_path, output_folder):
        fname = os.path.basename(video_path)
        if fname.endswith(('.MP4', '.mp4', '.avi')):
            output_name = fname if fname.lower().endswith('.mp4') else fname.replace('.avi', '.MP4')
            output_path = os.path.join(output_folder, output_name)
            cmd = f'ffmpeg -i "{video_path}" -vcodec h264_nvenc -cq 30 "{output_path}"'  # this is what we use across all videos
            # cmd = f'ffmpeg -i "{video_path}" -vcodec libx264 -crf 28 "{output_path}"'

        elif fname.endswith('.LRV'):
            output_name = fname.replace('.LRV', '.MP4')
            output_path = os.path.join(output_folder, output_name)
            cmd = f'ffmpeg -i "{video_path}" -vcodec libx264 -crf 28 "{output_path}"'
        else:
            raise (f"Unsupported file format: {fname}")

        try:
            subprocess.run(cmd, shell=True, check=True, text=True)
        except subprocess.CalledProcessError as e:
            logging.error(f'Error executing command: {cmd}\nError message: {e.stderr}')
            return False  # signal failure if any of the meta data extraction fails
        return fname  # signal success if compression succeeds

    # download tracking sheet as dataframe
    def sheet_to_dataframe(self, range_name=None):
        # THESE ARE THE HEADERS THAT ARE REQUIRED IN THE SHEET for the pipeline
        self.required_headers = {'Processed_date', 'Status', 'Duration', 'old_name', 'new_name',
                                 'new_location_raw','new_location_storage_zip', 'new_location_storage_mp4'}
        self.spreadsheet_id = '1mAti9dBNUqgNQQIIsnPb5Hu59ovKCUh9LSYOcQvzt2U'  # session tracking sheet
        # which sheet to download
        if self.args.bv_type == 'luna':
            self.range_name = 'Luna_Round_2_Ongoing'
        elif self.args.bv_type == 'main':
            self.range_name = 'Ongoing_data_collection'
        elif self.args.bv_type == 'bing':
            self.range_name = 'Bing'

        if settings.test_mode:
            self.range_name = f'{self.range_name}_test'

        try:
            # get the sheet info
            sheet = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id, range=self.range_name if not range_name else range_name
            ).execute()
            values = sheet.get('values', [])
            header = values[0] if values else []
            # pad the values with empty strings to make sure all rows have the same length
            padded_values = [row + [''] * (len(header) - len(row)) for row in values[1:]]
            # Create a pandas DataFrame from the padded values
            df = pd.DataFrame(padded_values, columns=header)
            assert self.required_headers.issubset(df.columns), \
                f"Missing required headers: {self.required_headers - set(df.columns)}. Please add them to the sheet."
            logging.info(f"Tracking sheet {self.range_name} loaded.")
        except Exception as e:
            df = pd.DataFrame()
            logging.info(f"Tracking sheet {self.range_name} not found: {e}")
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

    def download_file(self, service, file_id, file_path, video_info):
        directory, filename = os.path.split(file_path)
        video_id, extension = os.path.splitext(filename)
        fname_infos = os.path.dirname(os.path.relpath(file_path, self.args.video_root)).split('/')
        bv_main_folder = fname_infos[0]  # BabyView_Main, BabyView_Bing, BabyView_Play
        subject_id = fname_infos[1]
        # record_period = fname_infos[-1]

        if 'By_Date' in fname_infos:
            fname_infos.remove('By_Date')
        if 'By Date' in fname_infos:
            fname_infos.remove('By Date')

        if video_info['date'] == 'NA' or video_info['session_num'] == 'NA' or len(video_info['uniq_id']) != 10:
            raise ValueError(f"{video_info} has items not met requirements.")
        else:
            file_name = f'{subject_id}_{video_info['date']}_{video_info['session_num']}_{video_info['uniq_id']}{extension}'
        # if self.args.bv_type in ['main', 'luna']:
        #
        #     # with '/' to match the format in the sheet
        #     week = record_period.replace('.', '/')
        #     date, time = self.get_week_date_time_from_sheet(self.datetime_tracking, subject_id, video_id, week)
        #     # add created date to file name
        #     if date is None or time is None:
        #         create_date = service.files().get(
        #             fileId=file_id,
        #             fields='createdTime',
        #             supportsAllDrives=True
        #         ).execute()['createdTime']
        #         date_obj = datetime.strptime(create_date, "%Y-%m-%dT%H:%M:%S.%fZ")
        #         if date is None:
        #             date = date_obj.strftime('%Y-%m-%d')
        #         if time is None:
        #             time = date_obj.strftime('%H:%M:%S')
        #
        #     datetime_str = f'{date}-{time}'.replace(' ', '').replace('/', '.')
        #     file_name = f'{subject_id}_{video_id}_{record_period}_{datetime_str}{extension}'
        # else:
        #     file_name = f'{subject_id}_{video_id}_{record_period}{extension}'

        os.makedirs(directory, exist_ok=True)
        raw_path = os.path.join(directory, file_name).replace(' ', '_')
        # folder to store processed video & meta data
        processed_folder = os.path.join(self.args.output_folder, bv_main_folder, subject_id, video_id)
        if os.path.exists(raw_path):
            print(f"File already exists: {raw_path}")
            return raw_path, processed_folder

        print(f"Downloading to: {file_path}")
        if not os.path.exists(processed_folder):
            os.makedirs(processed_folder, exist_ok=True)
        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(raw_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}% complete.")
        self.total_video_count += 1
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

    def upload_files_storage_bucket(self, gcp_bucket_name, zip_path, video_path, common_folder):
        # upload video file
        gcp_video_gcp_path = video_path.split(common_folder)[-1]
        processed_vid_gcp_msg, processed_success = storage_client_instance.upload_file_to_gcs(
            source_file_name=video_path, destination_path=gcp_video_gcp_path, gcp_bucket=gcp_bucket_name
        )
        storage_client_instance.logs['processed_details'].append(processed_vid_gcp_msg)

        # zip file
        gcp_zip_gcp_path = zip_path.split(common_folder)[-1]
        zip_gcp_msg, zip_success = storage_client_instance.upload_file_to_gcs(
            source_file_name=zip_path, destination_path=gcp_zip_gcp_path, gcp_bucket=gcp_bucket_name
        )
        storage_client_instance.logs['zip_details'].append(zip_gcp_msg)

        return processed_success, zip_success

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
        return build(service_type, version, credentials=creds)

    def download_videos_from_drive(self):
        # get a list of file info to be downloaded from tracking sheet
        # returning file_id, path, subject_id, date, session_num, uniq_id, Processed_date, Status, Duration.
        # gcp_name,
        # Added check on the date, NA if date is not valid.
        if settings.test_video_info:
            downloading_file_info = settings.test_video_info
        else:
            downloading_file_info = self.get_downloading_file_paths()

        if self.args.bv_type == 'bing':
            entry_point_folder_name = "BabyView_Bing"
        elif self.args.bv_type in ['main', 'luna']:
            entry_point_folder_name = "BabyView_Main"

        # create raw and storage bucket if not exist.
        storage_bucket = f'{entry_point_folder_name}_storage'.lower()
        raw_bucket = f'{entry_point_folder_name}_raw'.lower()
        if raw_bucket not in self.gcs_buckets:
            logging.info(f"Creating {raw_bucket} bucket...")
            storage_client_instance.create_gcs_buckets(raw_bucket)

        if storage_bucket not in self.gcs_buckets:
            logging.info(f"Creating {storage_bucket} bucket...")
            storage_client_instance.create_gcs_buckets(storage_bucket)

        # print([item for item in downloading_file_info if item.get("file_id") is not None])

        for video_info in downloading_file_info:
            file_id = video_info['file_id']
            download_path = video_info['file_path']

            # Step 1. Download the raw video file if file id is available
            if file_id:
                download_path = os.path.join(self.args.video_root, entry_point_folder_name, download_path)
                download_folder = os.path.dirname(download_path).replace('By Date', 'By_Date')
                os.makedirs(download_folder, exist_ok=True)
                try:
                    raw_path, processed_folder = self.download_file(self.drive_service, file_id, download_path,
                                                                    video_info)
                except Exception as e:
                    logging.info(f"Failed to download {file_id}...{e}")
                    video_info['Status'] = 'Download failed'
                    continue
            else:
                logging.info(f"File id not available for {file_id}")
                video_info['Status'] = 'Not found'
                raw_path = None

            gcp_new_file_name = None

            # Step 2. Upload raw video file to GCS if download is successful. Next step is contingent 
            # on download success
            if raw_path:
                gcp_storage_raw_path = raw_path.split(f"{entry_point_folder_name}/")[1]
                gcp_new_file_name = raw_path.split('/')[-1].split('.')[0]
                print(gcp_storage_raw_path, raw_path, gcp_new_file_name)
                raw_upload_msg, raw_upload_success = storage_client_instance.upload_file_to_gcs(
                    source_file_name=raw_path,
                    destination_path=gcp_storage_raw_path,
                    gcp_bucket=raw_bucket
                )
                # raw_upload_msg, raw_upload_success = "Test", True
                storage_client_instance.logs['raw_details'].append(raw_upload_msg)
                if raw_upload_success:
                    video_info['new_location_raw'] = f"gs://{raw_bucket}/{gcp_storage_raw_path}"
                    storage_client_instance.logs['raw_success'] += 1
                else:
                    video_info['new_location_raw'] = "Raw_Upload_Failed"
                    storage_client_instance.logs['raw_failure'] += 1

                # Step 3. Extract meta from the raw video file and compress it, only process if raw upload is successful
                # process meta data
                if raw_upload_success:
                    # Check if the file has old gcp name existed, if so delete the old file from GCP bucket.
                    if video_info['gcp_name'] and video_info['Status'].lower() in ['update', 'delete']:
                        delete_blobs_msg = storage_client_instance.delete_blobs_with_substring(bucket_name=raw_bucket,
                                                                                               file_substring=
                                                                                               video_info[
                                                                                                   'gcp_name'])
                        storage_client_instance.logs['file_deletion_details'].append(delete_blobs_msg)

                    os.makedirs(processed_folder, exist_ok=True)
                    # LUNA avi videos do not have meta data, will just compress, but GoPro videos have metadata
                    if (self.args.bv_type == 'luna' and 'LUNA' in raw_path) or raw_path.endswith('LRV'):
                        video_ext = '.avi'
                        video_fname = self.compress_vid(raw_path, processed_folder)
                    else:
                        video_ext = '.MP4'
                        try:
                            video_fname = self.extract_meta(raw_path, processed_folder)
                            if not video_fname:
                                video_info['Status'] = 'Meta extraction failed'
                        except Exception as e:
                            logging.info(f">>>>>>>>>>>>>>>>>>>>>> {raw_path} failed to process..")
                            logging.info("Exception is", e)
                            video_fname = False
                            print(f"Process success {video_fname}...")
                            video_info['Status'] = 'Meta extraction failed'

                    # Step 4. Create a zip file of the processed folder and upload it and the video to GCS
                    try:
                        if video_fname:
                            zip_output_path = os.path.join(os.path.dirname(processed_folder), video_fname)
                            zip_output_path = zip_output_path.replace(video_ext, '')
                            zip_path, video_path = self.zip_files(processed_folder, zip_output_path)
                            print(f"Zipped {zip_path}...vid {video_path}...")
                            # upload the zip and mp4 to GCS
                            common_folder = f"{entry_point_folder_name}/"
                            processed_success, zip_success = self.upload_files_storage_bucket(
                                gcp_bucket_name=storage_bucket, zip_path=zip_path,
                                video_path=video_path, common_folder=common_folder
                            )
                            if processed_success:
                                storage_client_instance.logs['processed_success'] += 1
                                video_info['new_location_storage_mp4'] = f"gs://{storage_bucket}/{video_path}"
                            else:
                                storage_client_instance.logs['processed_failure'] += 1
                                video_info['new_location_storage_mp4'] = "Processed_Vid_Upload_Failed"

                            if zip_success:
                                storage_client_instance.logs['zip_success'] += 1
                                video_info['new_location_storage_zip'] = f"gs://{storage_bucket}/{zip_path}"
                            else:
                                storage_client_instance.logs['zip_failure'] += 1
                                video_info['new_location_storage_zip'] = "Zip_Upload_Failed"
                            if processed_success and zip_success:
                                video_info['Status'] = 'Uploaded'
                                if video_info['gcp_name'] and video_info['Status'].lower() in ['update', 'delete']:
                                    delete_blobs_msg = storage_client_instance.delete_blobs_with_substring(
                                        bucket_name=storage_bucket,
                                        file_substring=
                                        video_info[
                                            'gcp_name'])
                                    storage_client_instance.logs['file_deletion_details'].append(delete_blobs_msg)
                            else:
                                video_info['Status'] = 'Partially Uploaded'
                            # get video duration
                            video = VideoFileClip(video_path)
                            duration = video.duration
                            video_info['Duration'] = duration
                            video.close()
                            # remove the downloaded and processed files to save local storage
                            remove_processed_path = os.path.commonpath([zip_path, video_path])
                            print(f"Finished processing, removing {remove_processed_path}")
                            self.clear_directory_contents(remove_processed_path)
                            remove_raw_path = remove_processed_path.replace('processed', 'raw')
                            print(f"Finished processing, removing {remove_raw_path}")
                            self.clear_directory_contents(remove_raw_path)
                            shutil.rmtree(remove_raw_path)
                    except Exception as e:
                        print(f">>>>>>>>>>>>>>>>>>>>>> {video_fname} failed to upload..")
                        video_info['Status'] = 'Processed Upload failed'
                        print("Exception is", e)

                else:
                    video_info['Status'] = 'Raw upload failed'

            # Step 5. Update the video info with the processed date and duration on the tracking sheet
            if gcp_new_file_name:
                video_info['Processed_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                video_info['old_name'] = video_info['gcp_name']
                video_info['new_name'] = gcp_new_file_name
                row_idx = video_info['idx']
                columns = self.datetime_tracking.columns
                columns_str_idx_dict = {col: ascii_uppercase[idx] for idx, col in enumerate(columns)}
                start_str_idx = columns_str_idx_dict['Processed_date']
                end_str_idx = columns_str_idx_dict['new_location_storage_mp4']
                start_index = list(columns).index("Processed_date")
                end_index = list(columns).index("new_location_storage_mp4") + 1
                columns_in_range = list(columns)[start_index:end_index]
                range_name = f'{self.range_name}!{start_str_idx}{row_idx}:{end_str_idx}{row_idx}'
                body = {'values': [[video_info[col] if col in self.required_headers else '' for col in columns_in_range]]}
                self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=self.spreadsheet_id, range=range_name,
                    valueInputOption='RAW', body=body
                ).execute()
                print(video_info)
                print(body)

        # Step 6. Upload logs to GCS
        log_name = f"hs-babyview-upload-log-{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        storage_client_instance.upload_dict_to_gcs(
            data=storage_client_instance.logs, bucket_name="hs-babyview-logs", filename=log_name
        )

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
        shutil.make_archive(zip_out_name, 'zip', root_dir=zip_folder)
        video_path = os.path.join(zip_folder, [f for f in os.listdir(zip_folder) if f.endswith(".MP4")][0])
        return zipfile_path, video_path


def miscellaneous_features():
    babyview_main_raw_bucket = storage_client_instance.client.bucket(bucket_name='babyview_main_raw')
    babyview_bing_raw_bucket = storage_client_instance.client.bucket(bucket_name='babyview_bing_raw')
    babyview_main_storage_bucket = storage_client_instance.client.bucket(bucket_name='babyview_main_storage')
    babyview_bing_storage_bucket = storage_client_instance.client.bucket(bucket_name='babyview_bing_storage')
    bucket_list = [babyview_bing_raw_bucket, babyview_main_raw_bucket, babyview_main_storage_bucket,
                   babyview_bing_storage_bucket]
    checking_buckets = [storage_client_instance.client.bucket(bucket_name='babyview_videos_to_check_raw'),
                        storage_client_instance.client.bucket(bucket_name='babyview_videos_to_check_storage')]

    # util.update_names_on_google_sheet(gcp_bucket=babyview_main_raw_bucket, spreadsheet_name='BabyView Session Tracking', range_name='Luna_Round_2_Ongoing', mode='new_name')
    data = []
    for bucket in checking_buckets:
        print(f"{bucket.name}:{len(list(bucket.list_blobs()))}")
        unconverted_files = util.update_gcs_files(gcp_bucket=bucket, spreadsheet_name='BabyView Session Tracking',
                                                  sheet_name='Main_Release_1_Corrected', update_sheet=False)
        data.extend(unconverted_files)
    df = pd.DataFrame(data)
    df.to_csv("unconverted_files_1.csv", index=False)

    # df = pd.read_csv("unconverted_files.csv")
    # df["matching_row"] = ""
    # SEARCH_TABS = [
    #     "Ongoing_data_collection",
    #     "Main_Release_1_Corrected",
    # ]
    # spreadsheet = util.get_google_sheet_data(credentials_json='creds/hs-babyview-sa.json',
    #                                          spreadsheet_name='BabyView Session Tracking', full_spreadsheet=True)
    # sheets_data = {}
    # for tab_name in SEARCH_TABS:
    #     try:
    #         worksheet = spreadsheet.worksheet(tab_name)
    #         sheets_data[tab_name] = worksheet.get_all_values()  # Fetch all data at once
    #     except gspread.exceptions.WorksheetNotFound:
    #         print(f"Warning: Sheet '{tab_name}' not found. Skipping...")
    #
    # for index, row in df.iterrows():
    #     file_name = row["file_name"]
    #     subject_id, video_id, week = util.extract_ids_and_week(file_name)
    #
    #     if subject_id and video_id:
    #         matches = []  # List to store all matching rows
    #         # Search in pre-fetched sheets_data
    #         for tab_name, data in sheets_data.items():
    #             headers = data[0]  # First row as headers
    #             rows = data[1:]  # Rest are data rows
    #
    #             # Ensure we get the column index for 'Week' (case insensitive)
    #             try:
    #                 week_col_idx = [h.lower() for h in headers].index("week")
    #             except ValueError:
    #                 print(f"Warning: 'Week' column not found in {tab_name}. Skipping this sheet.")
    #                 continue  # Skip this tab if "Week" column is missing
    #
    #             for row_idx, row_data in enumerate(rows, start=2):  # Start from second row (1-based index)
    #                 if subject_id in row_data and video_id in row_data and week == row_data[week_col_idx]:
    #                     matches.append(f"{tab_name} - Row {row_idx}")
    #
    #         # Assign matching data to the CSV column
    #         df.at[index, "matching_row"] = "; ".join(matches) if matches else "no_matching"
    # df.to_csv("unconverted_files.csv", index=False)
    # print(f"Updated data written to unconverted_files.csv")

    # util.move_matching_files(source_bucket=storage_client_instance.client.bucket(bucket_name='babyview_main_storage'),
    #                          target_bucket=storage_client_instance.client.bucket(bucket_name='babyview_videos_to_check_storage'),
    #                          csv_file="unconverted_files.csv")


def main():
    # cred_folder = "/ccn2/u/ziyxiang/cloud_credentials/babyview"
    cred_folder = "creds"
    parser = argparse.ArgumentParser(description="Download videos from cloud services")
    parser.add_argument('--bv_type', type=str, default='main', choices=['main', 'bing', 'luna'],
                        help='Babyview Main or Bing')
    # @TODO: temporarily to run multiple processes for each subject
    parser.add_argument('--subject_id', type=str, default='all', help='Subject ID to download videos for')
    parser.add_argument('--video_root', type=str, default=settings.video_root)
    parser.add_argument('--csv_path', type=str, default='uploaded_videos.csv')
    parser.add_argument('--cred_folder', type=str, default=cred_folder)
    parser.add_argument('--output_folder', type=str, default=settings.output_folder)
    parser.add_argument('--error_log', type=str, default='error_log.txt')
    args = parser.parse_args()
    while True:
        user_input = input(f"You are now running in {'TEST' if settings.test_mode else 'PRODUCTION'} mode. Continue? "
                           f"(yes/no):").strip().lower()
        if user_input in ["y", "yes"]:
            break  # Continue loop
        elif user_input in ["n", "no"]:
            print("❌ Process terminated by user.")
            return  # Exit the function
        else:
            print("⚠️ Invalid input. Please enter 'yes' or 'no'.")

    downloader = GoogleDriveDownloader(args)
    downloader.download_videos_from_drive()


if __name__ == '__main__':
    main()
