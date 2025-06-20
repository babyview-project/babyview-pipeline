import json
from pyairtable import Api
import pandas as pd

import settings

with open(settings.airtable_access_token_path, "r") as file:
    airtable_access_token = json.load(file).get("token")

app_id = 'appQ7P6moc6knzYzN'
video_table_id = 'tblkRXMPT0hTIYZcu'
participant_table_id = 'tbl5YIcTCibyia5gJ'
blackout_table_id = 'tblkwKGuCzkrw6EN8'

class AirtableServices:
    airtable = None

    def __init__(self):
        self.airtable = Api(airtable_access_token)
        self.video_table = self.airtable.table(base_id=app_id, table_name=video_table_id)
        self.blackout_table = self.airtable.table(base_id=app_id, table_name=blackout_table_id)
        self.participant_dict = self.set_participant_dict_from_participant_table()

    def get_video_info_from_video_table(self, filter_key=None, filter_value=None):
        from main import VideoStatus
        status_filter = f"status != '{VideoStatus.PROCESSED}'"
        if filter_key and filter_value:
            if isinstance(filter_value, str):
                main_filter = f"{filter_key} = '{filter_value}'"
            elif isinstance(filter_value, list):
                main_filter = "OR(" + ", ".join([f"{filter_key} = '{value}'" for value in filter_value]) + ")"
            else:
                main_filter = ""
            if main_filter:
                formula = f"AND({main_filter}, {status_filter})"
            else:
                formula = status_filter
        elif filter_key:
            formula = f"AND(NOT({filter_key}), {status_filter})"
        else:
            formula = status_filter

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
