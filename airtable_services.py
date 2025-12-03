import json
from pyairtable import Api
import pandas as pd
from datetime import datetime, timedelta
import pytz
from typing import List, Dict, Any
from status_types import VideoStatus
import settings
from tqdm import tqdm

with open(settings.airtable_access_token_path, "r") as file:
    airtable_access_token = json.load(file).get("token")

app_id = 'appQ7P6moc6knzYzN'
video_table_id = 'tblkRXMPT0hTIYZcu'
participant_table_id = 'tbl5YIcTCibyia5gJ'
blackout_table_id = 'tblkwKGuCzkrw6EN8'
# Airtable table for databrary tokens
airtable_databrary_token_table_id = "tblguVNudD3vvkzVK"
release_table_id = 'tblVeWx2MbrXRa6o1'


class AirtableServices:
    airtable = None

    def __init__(self):
        self.airtable = Api(airtable_access_token)
        self.video_table = self.airtable.table(base_id=app_id, table_name=video_table_id)
        self.blackout_table = self.airtable.table(base_id=app_id, table_name=blackout_table_id)
        self.participant_dict = self.set_participant_dict_from_participant_table()
        self.databrary_token_table = self.airtable.table(base_id=app_id, table_name=airtable_databrary_token_table_id)
        self.release_table = self.airtable.table(app_id, release_table_id)

    def get_video_info_from_video_table(self, filter_key=None, filter_value=None):
        status_filter = f"AND(status != '{VideoStatus.PROCESSED}', status != '{VideoStatus.REMOVED}')"
        cutoff_date = (datetime.now(pytz.timezone("America/Los_Angeles")) - timedelta(days=7)).strftime("%Y-%m-%d")
        date_filter = (
            f"OR("
            f"IS_BEFORE({{logging_date}}, '{cutoff_date}'),"
            f"IS_SAME({{logging_date}}, '{cutoff_date}', 'day')"
            f")"
        )
        formula_parts = [status_filter, date_filter]
        if filter_key and filter_value:
            # if filter_key == "subject_id":
            #     main_filter = "OR(" + ",".join([f'{{subject_id}} = "{sid}"' for sid in filter_value]) + ")"
            if isinstance(filter_value, str):
                main_filter = f"{filter_key} = '{filter_value}'"
            elif isinstance(filter_value, list):
                main_filter = "OR(" + ", ".join([f"{filter_key} = '{value}'" for value in filter_value]) + ")"
            else:
                main_filter = ""

            if main_filter:
                formula_parts.append(main_filter)

        elif filter_key:
            # UseCase "pipeline_run_date" to be null, for all non-processed vids.
            formula_parts.append(f"NOT({{{filter_key}}})")

        formula = "AND(" + ", ".join(formula_parts) + ")"

        print(f"Using airtable formula {formula}")
        records = self.video_table.all(formula=formula)
        if not records:
            return pd.DataFrame()  # Return empty DataFrame if no record found
        for record in records:
            subject_id_list = record["fields"].get("subject_id", [])

            participant_id = subject_id_list[0] if subject_id_list else "Unknown"

            record["fields"]["subject_id"] = self.participant_dict.get(participant_id, None)
        df = pd.DataFrame([record["fields"] for record in records])

        return df

    def get_video_ids_for_a_release_set(self, release_name: str) -> List[str]:
        """
        From release table (tblVeWx2MbrXRa6o1):
          - find row(s) with Name == release_name
          - read the 'Videos' linked-record field
          - return list of linked Video record IDs
        """
        formula = f"{{Name}} = '{release_name}'"
        try:
            records = self.release_table.all(formula=formula)
        except Exception as e:
            print(f"get_video_ids_for_release error for {release_name}: {e}")
            return []

        if not records:
            print(f"No release rows found with Name='{release_name}'")
            return []

        linked_video_ids: List[str] = []
        for rec in records:
            fields = rec.get("fields", {})
            linked = fields.get("Videos", [])
            # 'Videos' is a linked record field → list of Video record IDs
            if isinstance(linked, list):
                linked_video_ids.extend(linked)

        linked_video_ids = list(dict.fromkeys(linked_video_ids))
        print(f"Found {len(linked_video_ids)} linked videos")
        if not linked_video_ids:
            print(f"No linked videos found for release '{release_name}'")
            return []

        # Filter videos by databrary_upload_date == empty
        filtered_ids: List[str] = []
        for vid_id in tqdm(
            linked_video_ids,
            desc=f"Checking videos for release {release_name}",
            unit="video",
        ):
            try:
                vrec = self.video_table.get(vid_id)
            except Exception as e:
                print(f"get_video_ids_for_release: failed to fetch video {vid_id}: {e}")
                continue
            vfields = vrec.get("fields", {})
            databrary_date = vfields.get("databrary_upload_date")

            # Airtable "empty" can be: missing key, None, or ""
            if not databrary_date:
                filtered_ids.append(vid_id)

        print(
            f"Release '{release_name}': {len(filtered_ids)} without databrary_upload_date."
        )
        return filtered_ids

    def get_videos_for_drive_soft_delete(self, days_old: int = 30, page_size: int = 100) -> List[Dict[str, Any]]:
        """
        Return Airtable 'Video' records that are ready for Google Drive soft deletion (move to trash):
          - status == 'successfully_processed'
          - pipeline_run_date <= (today - days_old)
          - google_drive_deletion_date is blank
        """
        tz = pytz.timezone("America/Los_Angeles")
        cutoff_date = (datetime.now(tz) - timedelta(days=days_old)).strftime("%Y-%m-%d")

        # Airtable formula
        # NOTE: adjust field names if your base uses different exact names or capitalization
        formula = (
            "AND("
            f"status = {VideoStatus.PROCESSED},"
            f"IS_BEFORE({{pipeline_run_date}}, '{cutoff_date}'),"
            f"NOT(google_drive_deletion_date)"
            ")"
        )
        print(f"Using airtable formula {formula}")
        # Paginate to be safe
        offset = None
        records: List[Dict[str, Any]] = []
        while True:
            page = self.video_table.all(formula=formula, page_size=page_size, offset=offset)
            records.extend(page)
            # pyairtable returns 'offset' as a key on the raw response; if using wrapper, adapt accordingly.
            # If your version doesn't return an offset in '.all()', you can just break here.
            try:
                offset = page.offset  # may not exist based on pyairtable version
            except Exception:
                break
            if not offset:
                break
        return records

    def mark_video_soft_deleted_today(self, video_unique_id: str):
        tz = pytz.timezone("America/Los_Angeles")
        today = datetime.now(tz).strftime("%Y-%m-%d")
        # You appear to use update by Airtable record id in your other helper; keep the same pattern:
        self.update_video_table_single_video(video_unique_id, {"google_drive_deletion_date": today})

    def get_blackout_data_by_video_id(self, video_id):
        formula = f"{{video_id}} = '{video_id}'"
        return self.blackout_table.all(formula=formula)

    def set_participant_dict_from_participant_table(self, subject_id=None, recording_id=None):
        participant_table = self.airtable.table(base_id=app_id, table_name=participant_table_id)

        participant_dict = {}

        if subject_id:
            formula = f"{{subject_id}} = '{subject_id}'"
            records = participant_table.all(formula=formula)
        elif recording_id:
            formula = f"{{recording_id}} = '{recording_id}'"
            records = participant_table.all(formula=formula)
        else:
            records = participant_table.all()

        for record in records:
            participant_dict[record['id']] = record["fields"]['subject_id']

        return participant_dict

    def update_video_table_single_video(self, video_unique_id, data):
        self.video_table.update(video_unique_id, data)

    def update_blackout_table_single_video(self, video_unique_id, data):
        self.blackout_table.update(video_unique_id, data)



airtable_services = AirtableServices()
