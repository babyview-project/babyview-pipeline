from google.oauth2 import service_account
from google.cloud import storage
import settings
from io import BytesIO
from tqdm import tqdm
import os
import logging
import json
import pandas as pd
from datetime import datetime


class ProgressBytesIO(BytesIO):
    def __init__(self, bytes_io, progress_bar):
        self._bytes_io = bytes_io
        self._progress_bar = progress_bar
        # Ensure we're starting from the beginning of the BytesIO stream
        self._bytes_io.seek(0)

    def read(self, size=-1):
        # Update the progress bar with the number of bytes read
        chunk = self._bytes_io.read(size)
        self._progress_bar.update(len(chunk))
        return chunk

    def seek(self, offset, whence=0):
        return self._bytes_io.seek(offset, whence)

    def tell(self):
        return self._bytes_io.tell()


class GCPStorageServices:
    creds = service_account.Credentials.from_service_account_file(settings.gcp_service_account_path)
    client = storage.Client(credentials=creds)

    def __init__(self):
        self.gcs_buckets = self.list_gcs_buckets()
        self.logs = {'raw_success': 0,
                     'raw_failure': 0,
                     'processed_success': 0,
                     'processed_failure': 0,
                     'zip_success': 0,
                     'zip_failure': 0,
                     'raw_details': {},
                     'processed_details': {},
                     'zip_details': {},
                     'file_deletion_details': {},
                     'bucket_create_failure': {},
                     }

    def upload_file_to_gcs(self, source_file_name, destination_path, gcp_bucket):
        try:
            # # Get the total file size

            bucket = self.client.bucket(gcp_bucket)
            blob = bucket.blob(destination_path)

            # Wrap your BytesIO object with ProgressBytesIO
            file_size = os.path.getsize(source_file_name)
            pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc='Uploading')

            with open(source_file_name, "rb") as fh:
                progress_io = ProgressBytesIO(fh, pbar)
                blob.upload_from_file(progress_io, timeout=600)

            pbar.close()
            msg = None
            success = True
        except Exception as e:
            msg = e
            success = False

        return success, msg

    def delete_blobs_with_substring(self, bucket_name, file_substring):
        try:
            # Get the bucket containing the blob
            bucket = self.client.bucket(bucket_name)

            # List all blobs in the bucket
            blobs = bucket.list_blobs()

            # Collect blobs that match the substring
            if isinstance(file_substring, str):
                matched_blobs = [blob for blob in blobs if file_substring in blob.name]
            elif isinstance(file_substring, list):
                matched_blobs = [blob for blob in blobs if any(sub in blob.name for sub in file_substring)]
            else:
                return False, f"{file_substring}_not_str_or_list"

            if not matched_blobs:
                return True, None

            # Delete matched blobs and collect their names
            deleted_blob_names = []
            for blob in matched_blobs:
                deleted_blob_names.append(blob.name)
                blob.delete()

            return True, f"{deleted_blob_names}_deleted_from_{bucket_name}."
        except Exception as e:
            return False, f"{file_substring}_delete_from_{bucket_name}_failed_{e}"

    def upload_dict_to_gcs(self, data: dict, bucket_name, filename):
        try:
            # Reference the specified bucket
            bucket = self.client.bucket(bucket_name)

            # Convert the dictionary to JSON
            json_data = json.dumps(data)

            # Create a blob object in the specified bucket
            blob = bucket.blob(filename)

            # Upload the JSON data
            blob.upload_from_string(json_data, content_type='application/json')
            msg = f"{filename} has been saved to {bucket_name}."
        except Exception as e:
            msg = f"{filename} failed to be saved to {bucket_name}."

        print(msg)
        return msg

    def create_gcs_buckets(self, bucket_name, location='US'):
        try:
            # Initialize the bucket object with desired properties
            bucket = self.client.bucket(bucket_name)
            bucket.storage_class = "STANDARD"
            bucket.iam_configuration.uniform_bucket_level_access_enabled = True
            bucket.iam_configuration.public_access_prevention = 'enforced'
            # Create the new bucket
            new_bucket = self.client.create_bucket(bucket, location=location)

            print(f"Bucket {new_bucket.name} created.")
        except Exception as e:
            msg = f"Failed to create bucket {bucket_name}. Reason: {e}"
            self.logs['bucket_create_failure'].append(msg)

    def check_gcs_buckets(self):
        # create raw and storage bucket if not exist.
        for folder_name in settings.google_drive_entry_point_folder_names:
            storage_bucket = f'{folder_name}_storage'.lower()
            raw_bucket = f'{folder_name}_raw'.lower()
            # black_out_bucket = f'{folder_name}_blackout'.lower()

            if raw_bucket not in self.gcs_buckets:
                logging.info(f"Creating {raw_bucket} bucket...")
                self.create_gcs_buckets(raw_bucket)

            if storage_bucket not in self.gcs_buckets:
                logging.info(f"Creating {storage_bucket} bucket...")
                self.create_gcs_buckets(storage_bucket)

            # if black_out_bucket not in self.gcs_buckets:
            #     logging.info(f"Creating {black_out_bucket} bucket...")
            #     self.create_gcs_buckets(black_out_bucket)

    def list_gcs_buckets(self):
        # List all buckets
        buckets = self.client.list_buckets()

        # Extract and print bucket names
        bucket_names = [bucket.name for bucket in buckets]

        return bucket_names

    def read_all_names_from_gcs_bucket(self, bucket_name):
        file_names = []
        try:
            # Get the bucket
            bucket = self.client.bucket(bucket_name)

            # List all objects in the bucket and get their names
            blobs = bucket.list_blobs()
            file_names = [blob.name for blob in blobs]
        except Exception as e:
            print("Error in read_all_names_from_gcs_bucket bucket '{}': {}".format(bucket_name, e))

        return file_names

    def move_matching_files(self, source_bucket_name, target_bucket_name, file_uniq_id=None):
        source_bucket = self.client.bucket(source_bucket_name)
        target_bucket = self.client.bucket(target_bucket_name)

        if file_uniq_id:
            msg = ''
            try:
                blobs = source_bucket.list_blobs()  # Get all objects in the source bucket
                for blob in blobs:
                    # Check if the blob name contains the specified substring
                    if file_uniq_id in blob.name:
                        # Copy the blob to the destination bucket
                        new_blob = source_bucket.copy_blob(blob, target_bucket, blob.name)
                        # Delete the original blob from the source bucket
                        blob.delete()
                        msg = msg + f"Moved {blob.name} from {source_bucket} to {target_bucket}. "
                if msg:
                    success = True
                else:
                    msg = f"No such file contains {file_uniq_id}"
                    success = False
            except Exception as e:
                msg = f"Failed to move file {file_uniq_id} from {source_bucket} to {target_bucket}. Error: {e}."
                success = False
            return success, msg
