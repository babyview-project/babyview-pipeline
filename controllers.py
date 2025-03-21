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

    def get_downloading_file_paths(self, video_info_from_tracking: pandas.DataFrame) -> tuple:
        downloading_file_info = []
        errors = []

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
    processed_folder_path = None

    def __init__(self, video_path, processed_folder_path):
        self.video_raw_path = video_path
        self.processed_folder_path = processed_folder_path

    def extract_meta(self):
        extract_success = True
        for meta in ALL_METAS:
            meta_path = os.path.join(self.processed_folder_path, f'{meta}_meta.txt')
            print(self.video_raw_path, meta_path)
            cmd = f'../gpmf-parser/gpmf-parser {self.video_raw_path} -f{meta} -a | tee {meta_path}'
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
            highlights, fname = self.get_highlight_and_device_id(self.video_raw_path, settings.output_folder)
            return highlights, fname
        else:
            return None, extract_success

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
        return highlights, self.compress_vid(video_path, output_folder)

    def compress_vid(self):
        fname = os.path.basename(video_path)
        if fname.endswith(('.MP4', '.mp4', '.avi')):
            output_name = fname if fname.lower().endswith('.mp4') else fname.replace('.avi', '.MP4')
            output_path = os.path.join(output_folder, output_name)
            if settings.is_h264_nvenc_available:
                cmd = f'ffmpeg -i "{video_path}" -vcodec h264_nvenc -cq 30 "{output_path}"'  # this is what we use across all videos
            else:
                cmd = f'ffmpeg -i "{video_path}" -vcodec libx264 -crf 28 "{output_path}"'

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


def zip_files(zip_folder, zip_out_name):
    zipfile_path = f"{zip_out_name}.zip"
    print(f"Archive {zip_folder} to {zipfile_path}")
    shutil.make_archive(zip_out_name, 'zip', root_dir=zip_folder)
    video_path = os.path.join(zip_folder, [f for f in os.listdir(zip_folder) if f.endswith(".MP4")][0])
    return zipfile_path, video_path


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
