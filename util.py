import pytz
import gspread
import pandas as pd
import uuid
import re
import os
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime


def generate_unique_id():
    """Generate a short unique hash (10 chars)"""
    return uuid.uuid4().hex[:10]


def extract_ids_and_week(file_name):
    match = re.match(r"(\d{8})_(G[XL]\d{6})_(\d{2}\.\d{2}\.\d{4}-\d{2}\.\d{2}\.\d{4})", file_name)
    if match:
        subject_id = match.group(1)
        video_id = match.group(2)
        week = match.group(3)
        # Convert week format from MM.DD.YYYY-MM.DD.YYYY to MM/DD/YYYY-MM/DD/YYYY
        formatted_week = week.replace(".", "/")
        return subject_id, video_id, formatted_week
    return None, None, None


def convert_file_name(gcp_storage_raw_path):
    old_file_name = gcp_storage_raw_path.split('/')[-1]

    if not old_file_name:
        return None, f"Unable to parse old_file_name for {gcp_storage_raw_path}"  # Handle cases where file name is empty

    old_file_name_parts = old_file_name.split('_')
    subject_id = old_file_name_parts[0]  # First 8 digits

    # Extract file extension (handles double extensions like .LRV.zip)
    extension_match = re.search(r'(\.\w+)(\.\w+)?$', old_file_name)  # Matches .ext or .ext1.ext2
    if extension_match:
        file_extension = extension_match.group(0)  # Full extension (e.g., .LRV.zip or .MP4)
        main_part = old_file_name[: -len(file_extension)]  # Remove extension from filename
    else:
        return None, f"Unable to parse orig_file_extension for {old_file_name}"

    # Extract date in MM.DD.YYYY format from last part of old_file_name
    last_part = old_file_name_parts[-1]  # Always use last part for date extraction
    date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', last_part)
    if date_match:
        date_mm_dd_yyyy = date_match.group(0)
        try:
            date_yyyy_mm_dd = datetime.strptime(date_mm_dd_yyyy, "%m.%d.%Y").strftime("%Y-%m-%d")
        except Exception as e:
            return None, f"Unable to extract valid date for {old_file_name}"
    else:
        return None, f"No date matching for {old_file_name}"

    uniq_id = generate_unique_id()  # Generate unique ID

    # Process LUNA files
    if 'LUNA' in old_file_name:
        session_match = re.search(r'H\d{2}M\d{2}S\d{2}', old_file_name)
        if session_match:
            session_num = session_match.group(0)
        else:
            return None, f"Unable to extract session number for {old_file_name}"

    # Process Bing & Regular files (Extract session number from 4th digit of video_id)
    else:
        video_id = old_file_name_parts[1]  # e.g., GX020051
        if len(video_id) >= 4:
            session_num = video_id[3]  # Extract 4th digit (0-based index)
        else:
            return None, f"Unable to extract session number for {old_file_name}"

    # Construct new file name
    new_file_name = f"{subject_id}_{date_yyyy_mm_dd}_{session_num}_{uniq_id}{file_extension}"

    return new_file_name, None


def get_google_sheet_data(credentials_json, spreadsheet_name, range_name='', full_spreadsheet=False):
    """
    Read data from Google Sheets with a given range name and return a DataFrame.
    """
    # Authenticate Google Sheets API
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_json, scope)
    client = gspread.authorize(creds)

    if full_spreadsheet:
        return client.open(spreadsheet_name)
    else:
        # Open spreadsheet
        sheet = client.open(spreadsheet_name).worksheet(range_name)

        # Get data as a list of lists
        data = sheet.get_all_values()

        # Convert to DataFrame
        df = pd.DataFrame(data[1:], columns=data[0])  # First row as column headers

        return df, sheet


# Generate new names
def create_new_name(row):
    if row['Processed_date'].strip() and row['old_name'].strip():
        subject_id = row['subject_id']
        try:
            date_str = pd.to_datetime(row['Date']).strftime('%Y-%m-%d')
        except Exception as e:
            date_str = row['Date']

        video_id = row['video_id']

        if 'LUNA' in video_id:
            session_num = video_id.split('_')[-1]
        elif '_' in video_id:
            fourth_letter = video_id[3] if len(video_id) > 3 else "X"  # Ensure at least 4 characters
            session_num = f'{fourth_letter}-{video_id.split("_")[-1]}'
        else:
            session_num = video_id[3] if len(video_id) > 3 else "X"  # Ensure at least 4 characters

        unique_id = generate_unique_id()
        print(f'processed {video_id}')
        return f"{subject_id}_{date_str}_{session_num}_{unique_id}"
    return ""  # Return empty string if 'Processed_date' is empty


def find_matching_row_from_google_sheet(blob, df: pd.DataFrame, range_name, original_filename, original_filename_parts):
    # Setup values from blob.
    blob_subject_id = original_filename_parts[0]
    if len(original_filename_parts[2]) < 7:
        if 'LUNA' in original_filename:
            blob_video_id = f"{original_filename_parts[1]}_{original_filename_parts[2]}_{original_filename_parts[3]}_{original_filename_parts[4]}_{original_filename_parts[5]}"
            blob_week = original_filename_parts[6]
        else:
            # videos with suffixes
            blob_video_id = f'{original_filename_parts[1]}_{original_filename_parts[2]}'
            blob_week = original_filename_parts[3]
    else:
        blob_video_id = original_filename_parts[1]
        blob_week = original_filename_parts[2]
    blob_update_date = blob.updated.astimezone(pytz.timezone("America/Los_Angeles")).strftime("%Y-%m-%d")

    # Setup values from tracking sheet.
    tracking_date = pd.to_datetime(df["Date"], errors="coerce")
    tracking_process_date = pd.to_datetime(df["Processed_date"], errors="coerce")

    blob_date_list = original_filename_parts[-1].split("-")[0].split(".")
    if len(blob_date_list) >= 3:
        blob_date = f'{blob_date_list[2]}-{blob_date_list[0]}-{blob_date_list[1]}'
    else:
        blob_date = 'NA'

    if 'Bing' in range_name:
        # For Bing, only match subject_id, video_id and Date
        matching_row = df[
            (df["subject_id"] == blob_subject_id) &
            (df["video_id"] == blob_video_id) &
            (tracking_date.dt.strftime('%Y-%m-%d') == blob_date)
            ]
    else:
        # First time matching with subject_id, video_id, Week, and Date and upload_dt.
        try:
            matching_row = df[
                (df["subject_id"] == blob_subject_id) &
                (df["video_id"] == blob_video_id) &
                (df["Week"] == blob_week.replace('.', '/')) &  # Convert Series to formatted string
                (tracking_date.dt.strftime('%Y-%m-%d') == blob_date) &
                (tracking_process_date.dt.strftime('%Y-%m-%d') == blob_update_date)
                ]
        except Exception as e:
            matching_row = pd.DataFrame()
            print(f"{original_filename} has error.")
    if matching_row.empty and 'Bing' not in range_name:
        matching_row = df[
            (df["subject_id"] == blob_subject_id) &
            (df["video_id"] == blob_video_id) &
            (df["Week"] == blob_week.replace('.', '/')) &  # Convert Series to formatted string
            (tracking_date.dt.strftime('%Y-%m-%d') == blob_date)
            ]
        if matching_row.empty:
            matching_row = df[
                (df["subject_id"] == blob_subject_id) &
                (df["video_id"] == blob_video_id) &
                (df["Week"] == blob_week.replace('.', '/')) &  # Convert Series to formatted string
                (tracking_process_date.dt.strftime('%Y-%m-%d') == blob_update_date)
                ]
            return matching_row
        else:
            return matching_row
    else:
        return matching_row


def update_names_on_google_sheet(gcp_bucket, spreadsheet_name, range_name, mode: str = 'new_name'):
    """
    Fetch data, generate new names, and update the Google Sheet with the new column.
    """
    df, sheet = get_google_sheet_data('creds/hs-babyview-sa.json', spreadsheet_name, range_name)

    # Ensure necessary columns exist
    required_columns = {'subject_id', 'Date', 'video_id', 'Processed_date'}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"Missing required columns in Google Sheet: {required_columns - set(df.columns)}")
    if mode == 'new_name':
        df['new_name'] = df.apply(create_new_name, axis=1)
        # Update the Google Sheet with the new column
        sheet.update([df.columns.tolist()] + df.values.tolist())
    elif mode == 'old_name':
        blobs = list(gcp_bucket.list_blobs(prefix=None))
        for blob in blobs:
            original_filename = os.path.basename(blob.name)  # Extract filename from full path
            original_filename_parts = original_filename.split('_')
            matching_row = find_matching_row_from_google_sheet(blob, df, range_name, original_filename,
                                                               original_filename_parts)
            if not matching_row.empty:
                file_extension = f'.{os.path.basename(blob.name).split('.')[-1]}'
                print(original_filename)
                df.at[matching_row.index[0], 'old_name'] = original_filename.split(file_extension)[0]

        df_to_update = df.fillna("")
        # Update Google Sheet without date columns
        sheet.update([df_to_update.columns.tolist()] + df_to_update.values.tolist())
    return df


def parse_filename(filename):
    """Extract subject_id, video_id, and date from the given filename."""
    pattern = r"([^/]+)_([^/]+)_(\d{2}\.\d{2}\.\d{4})"  # Matches subject_id, video_id, and date
    match = re.search(pattern, filename)

    if match:
        subject_id, video_id, date_str = match.groups()
        return subject_id, video_id, date_str.replace(".", "-")  # Convert date format
    return None, None, None


def update_gcs_files(gcp_bucket, spreadsheet_name, sheet_name, update_sheet: bool = True):
    # Load tracking data
    df, sheet = get_google_sheet_data('creds/hs-babyview-sa.json', spreadsheet_name, sheet_name)

    # Ensure required columns exist
    if 'new_name' not in df.columns:
        raise ValueError("Tracking sheet must contain 'new_name' column.")

    # Add new columns if they don’t exist
    for col in ['new_location_raw', 'new_location_storage_zip', 'new_location_storage_mp4']:
        if col not in df.columns:
            df[col] = ""
    # List all files in the bucket
    blobs = list(gcp_bucket.list_blobs(prefix=None))  # Get all files in the bucket
    unconverted_files = []
    for blob in blobs:
        original_filename = os.path.basename(blob.name)  # Extract filename from full path
        original_filename_parts = original_filename.split('_')
        if len(original_filename_parts[-1].split('.')[0]) != 10:
            extension_match = re.search(r'(\.\w+)(\.\w+)?$', original_filename)  # Matches .ext or .ext1.ext2
            if extension_match:
                file_extension = extension_match.group(0)  # Full extension (e.g., .LRV.zip or .MP4)
            else:
                file_extension = None

            # Find the corresponding row in the tracking sheet
            matching_row = df[
                (df["old_name"] == original_filename.split(file_extension)[0])
            ]

            if matching_row.empty:
                unconverted_files.append({"bucket": gcp_bucket.name, "file_name": original_filename})
                continue  # No match found in tracking sheet

            new_name = matching_row["new_name"].values[0]  # Get the new name from the sheet

            new_blob_name = "/".join(
                blob.name.split("/")[:-1]) + "/" + new_name + file_extension  # Keep the folder path

            # Print details before proceeding
            # print(f"\n🚀 Ready to rename:")
            # print(f"   📁 Old File: {blob.name}")
            # print(f"   🔄 New File: {new_blob_name}")
            # print(f"   🌍 Bucket: {gcp_bucket.name}")
            #
            # # Ask for user confirmation before proceeding
            # while True:
            #     user_input = input("Proceed with this rename? (yes/no): ").strip().lower()
            #     if user_input in ["y", "yes"]:
            #         break  # Continue loop
            #     elif user_input in ["n", "no"]:
            #         print("❌ Process terminated by user.")
            #         return  # Exit the function
            #     else:
            #         print("⚠️ Invalid input. Please enter 'yes' or 'no'.")

            # Rename (Copy + Delete)
            gcp_bucket.copy_blob(blob, gcp_bucket, new_blob_name)
            blob.delete()

            # Construct new GCS URL
            new_url = f"gs://{gcp_bucket.name}/{new_blob_name}"

            # Determine which column to update based on bucket name and file type
            if "raw" in gcp_bucket.name:
                df.at[matching_row.index[0], 'new_location_raw'] = new_url
            elif "storage" in gcp_bucket.name:
                if original_filename.split('.')[-1] == "zip":
                    df.at[matching_row.index[0], 'new_location_storage_zip'] = new_url
                else:
                    df.at[matching_row.index[0], 'new_location_storage_mp4'] = new_url

            print(f"Renamed: {blob.name} -> {new_blob_name}")
        else:
            pass
            # print(f'{original_filename} has been processed.')

    # Save updates to Google Sheet
    if update_sheet:
        sheet.update([df.columns.tolist()] + df.values.tolist())
        print(f"File processing completed and tracking sheet updated. converted {len(unconverted_files)} files.")
        return unconverted_files
    else:
        print(f'unconverted files in {gcp_bucket.name} are: {unconverted_files}')
        print(
            f'ratio for {gcp_bucket.name} is: {len(unconverted_files)}/{len(blobs)}, {len(unconverted_files) / len(blobs)}')
        return unconverted_files




