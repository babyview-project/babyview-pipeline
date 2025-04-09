import json
from pyairtable import Api
import pandas as pd

import settings

with open(settings.airtable_access_token_path, "r") as file:
    airtable_access_token = json.load(file).get("token")


class AirtableServices:
    airtable = None

    def __init__(self):
        self.airtable = Api(airtable_access_token)
        self.app_id = 'appQ7P6moc6knzYzN'
        self.video_table_id = 'tblkRXMPT0hTIYZcu'
        self.participant_table_id = 'tbl5YIcTCibyia5gJ'
        self.video_table = self.airtable.table(base_id=self.app_id, table_name=self.video_table_id)
        self.participant_dict = self.set_participant_dict_from_participant_table()

    def get_video_info_from_video_table(self, filter_key=None, filter_value=None):
        if filter_key and filter_value:
            if isinstance(filter_value, str):
                formula = f"{filter_key} = '{filter_value}'"
            elif isinstance(filter_value, list):
                formula = "OR(" + ", ".join([f"{filter_key} = '{value}'" for value in filter_value]) + ")"
            else:
                formula = ""
        elif filter_key:
            formula = f"NOT({filter_key})"
        else:
            formula = None

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

    def set_participant_dict_from_participant_table(self, subject_id=None, recording_id=None):
        participant_table = self.airtable.table(base_id=self.app_id, table_name=self.participant_table_id)

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



