import settings
from datetime import datetime
from dateutil import parser
import re
import os
import pandas as pd


class Video:
    google_drive_file_id = None
    google_drive_file_path = None
    google_drive_video_name = None

    unique_video_id = None
    subject_id = None
    gopro_video_id = None
    session_num = None
    dataset = None
    recording_week = None
    date = None
    start_time = None
    logging_date = None
    blackout_region = None
    pipeline_run_date = None
    status = None
    duration = None

    gcp_bucket_name = None
    gcp_raw_location = None

    meta_extract = None
    highlight = None
    device_id = None

    local_raw_download_path = None
    local_processed_folder = None
    compress_video_path = None
    zipped_file_path = None

    gcp_storage_zip_location = None
    gcp_storage_video_location = None

    def __init__(self, video_info: dict):
        self.unique_video_id = video_info.get('unique_video_id', None)
        self.subject_id = video_info.get('subject_id', '')
        self.gopro_video_id = video_info.get('gopro_video_id', '')
        self.dataset = video_info.get('dataset', '')
        self.recording_week = video_info.get('recording_week', None)
        self.date = video_info.get('date', None)
        self.start_time = video_info.get('start_time', None)
        self.logging_date = video_info.get('logging_date', None)
        blackout_region = video_info.get('blackout_region', None)
        pipeline_run_date = video_info.get('pipeline_run_date', None)
        self.blackout_region = blackout_region if isinstance(blackout_region, list) else None
        self.pipeline_run_date = None if pd.isna(pipeline_run_date) else pipeline_run_date
        self.status = video_info.get('status', '')
        # self.duration = video_info.get('duration_sec', None)

        self.set_google_drive_video_name()
        self.set_session_num()
        self.gcp_file_name = f"{self.subject_id}_{self.normalize_date(date=self.date, date_format='%Y-%m-%d')}_{self.session_num}_{self.unique_video_id}"
        self.gcp_bucket_name = settings.google_drive_entry_point_folder_names[1].lower() if 'bing' in self.dataset.lower() else settings.google_drive_entry_point_folder_names[0].lower()

    def to_dict(self):
        """Converts the Video object attributes into a dictionary."""
        return {attr: getattr(self, attr) for attr in vars(self) if not attr.startswith("__")}

    def set_session_num(self):
        if 'luna' in self.gopro_video_id.lower():
            self.session_num = self.gopro_video_id.split('_')[-1]
        else:
            self.session_num = self.gopro_video_id[3] if len(self.gopro_video_id) > 4 else None

    def normalize_date(self, date, date_format):
        try:
            # Attempt to parse the date
            return parser.parse(date).strftime(date_format)
        except Exception as e:
            print(f"Error when set_date_for_naming for {self.subject_id}_{self.gopro_video_id}: {date}. {e}")
            return None

    def set_google_drive_video_name(self):
        if 'luna' in self.gopro_video_id.lower():
            self.google_drive_video_name = f'{self.gopro_video_id}.avi'
        else:
            if self.gopro_video_id.startswith('GX'):
                self.google_drive_video_name = f'{self.gopro_video_id}.MP4'
            else:
                self.google_drive_video_name = f'{self.gopro_video_id}.LRV'

    def set_file_id_file_path(self, google_drive_service):
        """ Takes a list of folder names and the file name then returns the file ID """
        if 'bing' in self.dataset.lower():
            folder_id = "1-ATtN-wZ_mVY3Hm8Q0DO9CVizBsAmY6D"

            # google_drive_folder_path = [re.sub(r"\D", "", self.subject_id), self.normalize_date(self.date, "%m/%d/%Y")]
            google_drive_folder_path = [self.subject_id, self.normalize_date(self.date, "%m/%d/%Y")]
            gcp_folder_path = [self.subject_id, self.normalize_date(self.date, "%Y-%m-%d")]
        else:
            folder_id = "1ZfVyOBqb2L-Sw0b5himyg_ysB6Mwb8bo"
            drive_week = f"{self.normalize_date(self.recording_week.split('-')[0], "%m/%d/%Y")}-{self.normalize_date(self.recording_week.split('-')[1], "%m/%d/%Y")}"
            gcp_week = f"{self.normalize_date(self.recording_week.split('-')[0], "%Y.%m.%d")}-{self.normalize_date(self.recording_week.split('-')[1], "%Y.%m.%d")}"

            # google_drive_folder_path = [re.sub(r"\D", "", self.subject_id), 'By Date', drive_week]
            google_drive_folder_path = [self.subject_id, 'By Date', drive_week]
            gcp_folder_path = [self.subject_id, "By_Date", gcp_week]

        kwargs = dict(
            driveId=settings.babyview_drive_id,
            corpora='drive',
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            fields="files(id, name)"
        )
        for folder_name in google_drive_folder_path:
            query = f"'{folder_id}' in parents and name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder'"
            results = google_drive_service.files().list(q=query, **kwargs).execute()
            items = results.get('files', [])
            if not items:
                return f'{self.unique_video_id}_{self.subject_id}_{self.gopro_video_id}_drive_folder_"{folder_name}"_not_found.'

            folder_id = items[0]['id']

        query = f"'{folder_id}' in parents and name = '{self.google_drive_video_name}'"
        results = google_drive_service.files().list(q=query, **kwargs).execute()
        items = results.get('files', [])
        if not items:
            return f'{self.unique_video_id}_{self.subject_id}_{self.gopro_video_id}_drive_video_"{self.google_drive_video_name}"_not_found'

        self.google_drive_file_id = items[0]["id"]
        self.google_drive_file_path = "/".join(google_drive_folder_path + [self.google_drive_video_name])
        self.gcp_raw_location = f"{'/'.join(gcp_folder_path + [self.gcp_file_name])}{os.path.splitext(self.google_drive_video_name)[1]}"

        return None
