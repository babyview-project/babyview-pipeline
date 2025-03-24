import os
import io
import re
import shutil
import traceback
import logging

import pandas

import settings
import util
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
from video import Video

# all meta data types that we want to extract
ALL_METAS = [
    'ACCL', 'GYRO', 'SHUT', 'WBAL', 'WRGB', 'ISOE',
    'UNIF', 'FACE', 'CORI', 'MSKP', 'IORI', 'GRAV',
    'WNDM', 'MWET', 'AALP', 'LSKP'
]
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('./log.txt'),
        logging.StreamHandler()
    ]
)
logging.basicConfig(
    filename=settings.error_log, filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.ERROR
)

storage_client_instance = GCPStorageServices()


class GoogleDriveDownloader:
    def __init__(self):
        self.SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive']
        self.drive_service = self.build_google_drive_service(service_type='drive')

    def build_google_drive_service(self, service_type='drive'):
        creds = None
        if os.path.exists(settings.google_api_token_path):
            creds = Credentials.from_authorized_user_file(settings.google_api_token_path, self.SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(settings.google_api_credential_path, self.SCOPES)
                creds = flow.run_local_server()
            with open(settings.google_api_token_path, 'w') as token:
                token.write(creds.to_json())
        version = 'v3' if service_type == 'drive' else 'v4'
        return build(service_type, version, credentials=creds)

    def get_downloading_file_paths(self, video_info_from_tracking: pd.DataFrame) -> tuple:
        downloading_file_info = []
        errors = []
        # for video_info in video_info_from_tracking:
        for _, video_info in video_info_from_tracking.iterrows():  # Iterate over DataFrame rows
            video = Video(video_info=video_info.to_dict())  # Convert row to dictionary
            error_msg = video.set_file_id_file_path(google_drive_service=self.drive_service)

            if video.google_drive_file_id:
                downloading_file_info.append(video)
            else:
                errors.append(error_msg)

        return downloading_file_info, errors

    def download_file(self, local_raw_download_folder, video: Video):
        try:
            print(f'Downloading {video.subject_id}_{video.gopro_video_id} to {local_raw_download_folder}.')
            request = self.drive_service.files().get_media(fileId=video.google_drive_file_id)
            fh = io.FileIO(local_raw_download_folder, 'wb')
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                print(f"Download {int(status.progress() * 100)}% complete.")

            return done, None
        except Exception as e:
            return False, e


class FileProcessor:
    video_raw_path = None
    processed_folder = None

    def __init__(self, video_raw_path, processed_folder):
        self.video_raw_path = video_raw_path
        self.processed_folder = processed_folder

    def extract_meta(self):
        error_msg = None
        output_text_list = []
        for meta in ALL_METAS:
            meta_path = os.path.join(self.processed_folder, 'meta_data', f'{meta}_meta.txt')
            cmd = f'../gpmf-parser/gpmf-parser {self.video_raw_path} -f{meta} -a | tee {meta_path}'
            try:
                result = subprocess.run(cmd, shell=True, check=True, capture_output=True, timeout=120)
                try:
                    output_text = result.stdout.decode('utf-8')
                    output_text_list.append(output_text)
                except UnicodeDecodeError:
                    output_text = result.stdout.decode('utf-8', 'replace')  # Replace or ignore invalid characters
                    output_text_list.append(output_text)
                if 'error' in output_text.lower():
                    error_msg = f'Error executing command: {cmd}\nError message: {output_text}'
                    output_text_list = []
                    break
            # something is wrong with the video file
            except subprocess.CalledProcessError as e:
                error_msg = f'Error executing command: {cmd}\nError message: {e.stderr}'
                # signal failure if any of the meta data extraction fails
                output_text_list = []
                break
            except subprocess.TimeoutExpired:
                error_msg = f"Command timed out: {cmd}"
                output_text_list = []
                break
            except Exception as e:
                error_msg = f'Unexpected error while executing {cmd}: {traceback.format_exc()}, {e}'
                output_text_list = []
                break
        # no need to compress if meta data extraction fails (video corrupted)
        return output_text_list, error_msg

    def get_highlight_and_device_id(self):
        def save_info(file_name, all_info, output_path, info_type):
            assert info_type in ['highlights', 'device_id'], \
                'info_type needs to be either device_id or highlights'
            str2insert = ""
            str2insert += file_name + "\n"
            if info_type == 'highlights':
                for i, highl in enumerate(all_info):
                    str2insert += "(" + str(i + 1) + "): "
                    str2insert += sec2dtime(highl) + "\n"
            elif info_type == 'device_id':
                str2insert += all_info
            str2insert += "\n"
            with open(output_path, "w") as f:
                f.write(str2insert)

        highlights = None
        device_id = None
        msg = None
        file_name = os.path.basename(self.video_raw_path).split('.')[0]
        try:
            highlights = examine_mp4(self.video_raw_path)
            highlights.sort()
            highlight_path = os.path.join(self.processed_folder, 'highlights_device_info',
                                          f'GP-Highlights_{file_name}.txt')
            save_info(file_name, highlights, highlight_path, 'highlights')
            device_id = device.examine_mp4(self.video_raw_path)
            device_id_path = os.path.join(self.processed_folder, 'highlights_device_info',
                                          f'GP-Device_name_{file_name}.txt')
            save_info(file_name, device_id, device_id_path, 'device_id')
        except Exception as e:
            msg = f"Error in get_highlight_and_device_id from {file_name}: {e}"
        return highlights, device_id, msg

    def compress_vid(self):
        fname = os.path.basename(self.video_raw_path)
        extension = os.path.splitext(fname)[1]

        if extension.lower() not in ['.mp4', '.avi', '.lrv']:
            return None, f"Unsupported file format: {fname}"

        output_name = fname.replace(extension, '.mp4')
        output_path = os.path.join(self.processed_folder, output_name)

        # Choose codec based on file type and availability
        if extension.lower() in ['.mp4', '.avi'] and settings.is_h264_nvenc_available:
            codec = "-vcodec h264_nvenc -cq 30"
        else:
            codec = "-vcodec libx264 -crf 28"

        cmd = f'ffmpeg -i "{self.video_raw_path}" {codec} "{output_path}"'

        try:
            subprocess.run(cmd, shell=True, check=True, text=True)
        except subprocess.CalledProcessError as e:
            msg = f'Error executing command: {cmd}\nError message: {e.stderr}'
            return None, msg

        return output_path, None  # Success

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

    def zip_files(self):
        error = None
        zipfile_path = None

        try:
            meta_data_folder = os.path.join(self.processed_folder, 'meta_data')
            zipfile_base = os.path.join(self.processed_folder, 'meta_data')  # no .zip here

            # Create the zip file
            zipfile_path = shutil.make_archive(base_name=zipfile_base, format='zip', root_dir=meta_data_folder)
        except Exception as e:
            error = e

        return zipfile_path, error


def clear_directory_contents(dir_path):
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
