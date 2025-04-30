import json
import os
import io
import shutil
import traceback
import logging
import struct
import numpy as np
from math import floor

import settings
import util
import subprocess
import pandas as pd

from tqdm import tqdm
from datetime import datetime
from string import ascii_uppercase
from typing import List, Dict, Any

from pathlib import Path
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

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
    video = None

    def __init__(self, video: Video):
        self.video = video

    def extract_meta(self):
        """Extract specified telemetry tags from a GoPro video and write to separate files."""
        error_msg = None
        output_text_list = []

        for meta in ALL_METAS:
            meta_path = os.path.join(self.video.local_processed_folder, f'{self.video.gcp_file_name}_metadata', f'{meta}_meta.txt')

            cmd = f'{settings.gpmf_parser_location} {self.video.local_raw_download_path} -f{meta} -a | tee {meta_path}'
            try:
                result = subprocess.run(cmd, shell=True, check=True, capture_output=True, timeout=120)
                try:
                    output_text = result.stdout.decode('utf-8')
                    output_text_list.append(output_text)
                except UnicodeDecodeError:
                    output_text = result.stdout.decode('utf-8', 'replace')  # Replace or ignore invalid characters
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

    @staticmethod
    def find_boxes(f, start_offset=0, end_offset=float("inf")):
        """Returns a dictionary of all the data boxes and their absolute starting
        and ending offsets inside the mp4 file.

        Specify a start_offset and end_offset to read sub-boxes.
        """
        s = struct.Struct("> I 4s")
        boxes = {}
        offset = start_offset
        f.seek(offset, 0)
        while offset < end_offset:
            data = f.read(8)  # read box header
            if data == b"": break  # EOF
            length, text = s.unpack(data)
            f.seek(length - 8, 1)  # skip to next box
            boxes[text] = (offset, offset + length)
            offset += length
        return boxes

    def examine_mp4(self, filename):

        with open(filename, "rb") as f:
            boxes = self.find_boxes(f)

            # Sanity check that this really is a movie file.
            def fileerror():  # function to call if file is not a movie file
                print("")
                print("ERROR, file is not a mp4-video-file!")

                os.system("pause")
                exit()

            try:
                if boxes[b"ftyp"][0] != 0:
                    fileerror()
            except:
                fileerror()

            moov_boxes = self.find_boxes(f, boxes[b"moov"][0] + 8, boxes[b"moov"][1])

            udta_boxes = self.find_boxes(f, moov_boxes[b"udta"][0] + 8, moov_boxes[b"udta"][1])

            if b'GPMF' in udta_boxes.keys():
                ### get GPMF Box
                highlights = self.parse_highlights(f, udta_boxes[b'GPMF'][0] + 8, udta_boxes[b'GPMF'][1])
            else:
                # parsing for versions before Hero6
                highlights = self.parse_highlights_old_version(f, udta_boxes[b'HMMT'][0] + 12, udta_boxes[b'HMMT'][1])

            print("")
            print("Filename:", filename)
            print("Found", len(highlights), "Highlight(s)!")

            return highlights

    @staticmethod
    def parse_highlights_old_version(f, start_offset=0, end_offset=float("inf")):
        listOfHighlights = []

        offset = start_offset
        f.seek(offset, 0)

        while True:
            data = f.read(4)

            timestamp = int.from_bytes(data, "big")

            if timestamp != 0:
                listOfHighlights.append(timestamp)
            else:
                break

        return np.array(listOfHighlights) / 1000  # convert to seconds and return

    @staticmethod
    def parse_highlights(f, start_offset=0, end_offset=float("inf")):

        inHighlights = False
        inHLMT = False
        skipFirstMANL = True

        listOfHighlights = []

        offset = start_offset
        f.seek(offset, 0)

        def read_highlight_and_append(f, list):
            data = f.read(4)
            timestamp = int.from_bytes(data, "big")

            if timestamp != 0:
                list.append(timestamp)

        while offset < end_offset:
            data = f.read(4)  # read box header
            if data == b"": break  # EOF

            if data == b'High' and inHighlights == False:
                data = f.read(4)
                if data == b'ligh':
                    inHighlights = True  # set flag, that highlights were reached

            if data == b'HLMT' and inHighlights == True and inHLMT == False:
                inHLMT = True  # set flag that HLMT was reached

            if data == b'MANL' and inHighlights == True and inHLMT == True:

                currPos = f.tell()  # remember current pointer/position
                f.seek(currPos - 20)  # go back to highlight timestamp

                data = f.read(4)  # readout highlight
                timestamp = int.from_bytes(data, "big")  # convert to integer

                if timestamp != 0:
                    listOfHighlights.append(timestamp)  # append to highlightlist

                f.seek(currPos)  # go forward again (to the saved position)

        return np.array(listOfHighlights) / 1000  # convert to seconds and return

    @staticmethod
    def sec2dtime(secs):
        """converts seconds to datetimeformat"""
        milsec = (secs - floor(secs)) * 1000
        secs = secs % (24 * 3600)
        hour = secs // 3600
        secs %= 3600
        min = secs // 60
        secs %= 60

        return "%d:%02d:%02d.%03d" % (hour, min, secs, milsec)

    def highlight_detection(self):
        highlights = []
        msg = None
        try:
            arr = self.examine_mp4(filename=self.video.local_raw_download_path)
            highlights = arr.tolist() if arr.size > 0 else []
            print('Here are all Highlights: ', highlights)
        except Exception as e:
            msg = f"Error in highlight_detection for {self.video.local_raw_download_path}: {e}"
        return highlights, msg

    def compress_vid(self):
        fname = os.path.basename(self.video.local_raw_download_path)
        extension = os.path.splitext(fname)[1]

        if extension.lower() not in ['.mp4', '.avi', '.lrv']:
            return None, f"Unsupported file format: {fname}"

        output_name = fname.replace(extension, '.mp4')
        output_path = os.path.join(self.video.local_processed_folder, output_name)

        # Choose codec based on file type and availability
        if extension.lower() in ['.mp4', '.avi'] and settings.is_h264_nvenc_available:
            codec = "-vcodec h264_nvenc -cq 30"
        else:
            codec = "-vcodec libx264 -crf 28"

        cmd = f'ffmpeg -i "{self.video.local_raw_download_path}" {codec} "{output_path}"'

        try:
            subprocess.run(cmd, shell=True, check=True, text=True)
        except subprocess.CalledProcessError as e:
            msg = f'Error executing command: {cmd}\nError message: {e.stderr}'
            return None, msg

        return output_path, None  # Success

    def get_video_duration(self):
        try:
            # get video duration
            result = subprocess.run([
                'ffprobe',
                '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'format=duration',
                '-of', 'json',
                self.video.local_raw_download_path
            ], capture_output=True, text=True)

            data = json.loads(result.stdout)
            duration_str = data['format']['duration']
            duration = round(float(duration_str), 2)
            return duration
        except Exception as e:
            print(
                f"Fail to get_video_duration from {self.video.local_raw_download_path}: {e}")
            return 0

    def zip_files(self):
        error = None
        zipfile_path = None

        try:
            # meta_data_folder = os.path.join(self.processed_folder, 'meta_data')
            # zipfile_base = os.path.join(self.processed_folder, 'meta_data')  # no .zip here
            local_processed_meta_data_folder = os.path.join(self.video.local_processed_folder,
                                                            f'{self.video.gcp_file_name}_metadata')

            # Create the zip file
            zipfile_path = shutil.make_archive(base_name=local_processed_meta_data_folder, format='zip',
                                               root_dir=local_processed_meta_data_folder)
        except Exception as e:
            error = e

        return zipfile_path, error

    def clear_directory_contents_raw_storage(self):
        """Remove everything inside raw and processed folders safely."""

        def safe_clear_dir(folder_path):
            if not folder_path:
                print("Provided folder path is None.")
                return
            if not os.path.isdir(folder_path):
                print(f"The specified folder does not exist or is not a directory: {folder_path}")
                return

            for filename in os.listdir(folder_path):
                file_path = os.path.join(folder_path, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.remove(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print(f"Failed to delete {file_path}. Reason: {e}")

        try:
            raw_folder = Path(self.video.local_raw_download_path).parents[
                2] if self.video.local_raw_download_path else None
            processed_folder = Path(self.video.local_processed_folder).parents[
                1] if self.video.local_processed_folder else None
        except Exception as e:
            print(f"Error resolving folder paths: {e}")
            return

        safe_clear_dir(str(raw_folder) if raw_folder else None)
        safe_clear_dir(str(processed_folder) if processed_folder else None)


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
