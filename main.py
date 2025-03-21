import argparse
import os
import logging
import settings
from controllers import GoogleDriveDownloader
from airtable_services import AirtableServices
from gcp_storage_services import GCPStorageServices

airtable = AirtableServices()
downloader = GoogleDriveDownloader()
storage = GCPStorageServices()


def make_local_directory(video):
    entry_point = settings.google_drive_entry_point_folder_names[1] if 'bing' in video.dataset.lower() else \
        settings.google_drive_entry_point_folder_names[0]
    local_raw_download_path = os.path.join(settings.raw_file_root, entry_point, video.gcp_raw_location)
    local_raw_download_folder = os.path.dirname(local_raw_download_path)
    local_processed_folder = os.path.join(settings.process_file_root, entry_point, video.subject_id,
                                          video.gopro_video_id)
    if not os.path.exists(local_raw_download_folder):
        os.makedirs(local_raw_download_folder, exist_ok=True)

    if not os.path.exists(local_processed_folder):
        os.makedirs(local_processed_folder, exist_ok=True)

    return local_raw_download_path, local_processed_folder


def process():
    logs = {}

    video_tracking_data = airtable.get_video_info_from_video_table(filter_key='unique_video_id',
                                                                   filter_value=['rec00KRZq9bT8l8nc',
                                                                                 'rec03VOaeG6dftcIg',
                                                                                 'rec0NwEqXa9gYtbyX',
                                                                                 'recsc7rperGsfmWSw'])
    if video_tracking_data.empty:
        logs['airtable'] = "No_Record_From_Airtable."
    else:
        logs['airtable'] = f"{len(video_tracking_data)}_Loaded"

        downloading_file_info, downloading_file_info_log = downloader.get_downloading_file_paths(
            video_info_from_tracking=video_tracking_data)
        logs['loading_download_info_error'] = downloading_file_info_log
        print(logs)

        for video in downloading_file_info:
            print(video.to_dict())
            # Step 0 Check if video needs to be deleted from buckets.
            if video.status.lower() in ['update', 'delete']:
                delete_success = []
                for gcp_bucket in [f"{video.gcp_bucket_name}_raw", f"{video.gcp_bucket_name}_storage", f"{video.gcp_bucket_name}_blackout"]:
                    success, delete_msg = storage.delete_blobs_with_substring(bucket_name=gcp_bucket, file_substring=video.unique_video_id)
                    delete_success.append(success)
                    if delete_msg:
                        logs['file_deletion'].append(delete_msg)
                if video.status.lower() == 'delete' and any(d==True for d in delete_success):
                    video.status = 'Removed on GCP'
                    continue

            # Step 1. Download the raw video file if file id is available
            raw_download_success = False
            if video.google_drive_file_id:
                local_raw_download_path, local_processed_folder = make_local_directory(video)

                raw_download_success, raw_download_error_msg = downloader.download_file(
                    local_raw_download_folder=local_raw_download_path, video=video)
                if raw_download_error_msg:
                    logs['raw_download_fail'].append(
                        f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}_{raw_download_error_msg}')
                    video.status = 'Raw download failed'
            else:
                logs['raw_download_fail'].append(
                    f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}_no_drive_file_id')
                local_raw_download_path = None
                video.status = 'Not found'

            # Step 2. Upload raw video file to GCS if download is successful.
            raw_upload_success = False
            if raw_download_success:
                if video.blackout_region:
                    raw_gcp_bucket = f"{video.gcp_bucket_name}_blackout"
                else:
                    raw_gcp_bucket = f"{video.gcp_bucket_name}_raw"
                print(f"Uploading {video.gcp_file_name} to {raw_gcp_bucket}/{video.gcp_raw_location}")

                raw_upload_success, raw_upload_error_msg = storage.upload_file_to_gcs(
                    source_file_name=local_raw_download_path,
                    destination_path=video.gcp_raw_location,
                    gcp_bucket=raw_gcp_bucket)
                # raw_upload_msg, raw_upload_success = "Test", True
                if raw_upload_error_msg:
                    logs['raw_upload_fail'].append(
                        f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}_{raw_upload_error_msg}')
                    video.status = 'Raw upload failed'
    #
            # Step 3. Extract meta from the raw video file and compress it, only process if raw upload is successful
            if raw_upload_success and not video.blackout_region:
                # LUNA avi videos do not have meta data, will just compress, but GoPro videos have metadata
               if 'LUNA' in video.gopro_video_id or raw_path.endswith('LRV'):
                    video_fname = self.compress_vid(raw_path, processed_folder)
                else:
                    video_ext = '.MP4'
                    try:
                        highlights, video_fname = self.extract_meta(raw_path, processed_folder)
                        inputs = input(highlights)
                        if not video_fname:
                            video_info['Status'] = 'Meta extraction failed'
                    except Exception as e:
                        logging.info(f">>>>>>>>>>>>>>>>>>>>>> {raw_path} failed to process..")
                        logging.info("Exception is", e)
                        print(f"Process success {video_fname}...")
                        video_info['Status'] = 'Meta extraction failed'
    #                 if highlights:
    #                     move_highlights_success, msg = util.move_matching_files(source_bucket=raw_bucket,
    #                                                                             target_bucket=black_out_bucket,
    #                                                                             file_uniq_id=video_info['uniq_id'])
    #                     storage_client_instance.logs['raw_details'].append(msg)
    #                     if move_highlights_success:
    #                         video_info['new_location_raw'] = f"gs://{black_out_bucket}/{gcp_storage_raw_path}"
    #                         storage_client_instance.logs['raw_success'] += 1
    #                     else:
    #                         video_info['new_location_raw'] = "Highlight_Video_Migration_Failed"
    #                         storage_client_instance.logs['raw_failure'] += 1
    #
    #             # Step 4. Create a zip file of the processed folder and upload it and the video to GCS
    #             try:
    #                 if video_fname and not highlights:
    #                     zip_output_path = os.path.join(os.path.dirname(processed_folder), video_fname)
    #                     zip_output_path = zip_output_path.replace(video_ext, '')
    #                     zip_path, video_path = self.zip_files(processed_folder, zip_output_path)
    #                     print(f"Zipped {zip_path}...vid {video_path}...")
    #                     # upload the zip and mp4 to GCS
    #                     common_folder = f"{entry_point_folder_name}/"
    #                     processed_success, zip_success = self.upload_files_storage_bucket(
    #                         gcp_bucket_name=storage_bucket, zip_path=zip_path,
    #                         video_path=video_path, common_folder=common_folder
    #                     )
    #                     if processed_success:
    #                         storage_client_instance.logs['processed_success'] += 1
    #                         video_info[
    #                             'new_location_storage_mp4'] = f"gs://{storage_bucket}/{video_path.split(common_folder)[-1]}"
    #                     else:
    #                         storage_client_instance.logs['processed_failure'] += 1
    #                         video_info['new_location_storage_mp4'] = "Processed_Vid_Upload_Failed"
    #
    #                     if zip_success:
    #                         storage_client_instance.logs['zip_success'] += 1
    #                         video_info[
    #                             'new_location_storage_zip'] = f"gs://{storage_bucket}/{zip_path.split(common_folder)[-1]}"
    #                     else:
    #                         storage_client_instance.logs['zip_failure'] += 1
    #                         video_info['new_location_storage_zip'] = "Zip_Upload_Failed"
    #                     if processed_success and zip_success:
    #                         video_info['Status'] = 'Uploaded'
    #                         if video_info['current_gcp_name'] and video_info['is_name_change_needed']:
    #                             delete_blobs_msg = storage_client_instance.delete_blobs_with_substring(
    #                                 bucket_name=storage_bucket,
    #                                 file_substring=
    #                                 video_info[
    #                                     'current_gcp_name'])
    #                             print(delete_blobs_msg)
    #                             storage_client_instance.logs['file_deletion_details'].append(delete_blobs_msg)
    #                     else:
    #                         video_info['Status'] = 'Partially Uploaded'
    #                     # get video duration
    #                     video = VideoFileClip(video_path)
    #                     duration = video.duration
    #                     video_info['Duration'] = duration
    #                     video.close()
    #                     # remove the downloaded and processed files to save local storage
    #                     remove_processed_path = os.path.commonpath([zip_path, video_path])
    #                     print(f"Finished processing, removing {remove_processed_path}")
    #                     self.clear_directory_contents(remove_processed_path)
    #                     remove_raw_path = remove_processed_path.replace('processed', 'raw')
    #                     print(f"Finished processing, removing {remove_raw_path}")
    #                     self.clear_directory_contents(remove_raw_path)
    #                     shutil.rmtree(remove_raw_path)
    #             except Exception as e:
    #                 print(f">>>>>>>>>>>>>>>>>>>>>> {video_fname} failed to upload..")
    #                 video_info['Status'] = 'Processed Upload failed'
    #                 print("Exception is", e)
    #
    #         elif video_info['black_out']:
    #             video_info['Status'] = 'Uploaded To Blackout'
    #         else:
    #             video_info['Status'] = 'Raw Upload Failed'
    #
    #     # Step 5. Update the video info with the processed date and duration on the tracking sheet
    #     if gcp_new_file_name:
    #         video_info['Processed_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #         video_info['old_name'] = video_info['current_gcp_name']
    #         video_info['new_name'] = gcp_new_file_name
    #         row_idx = video_info['idx']
    #         columns = self.datetime_tracking.columns
    #         columns_str_idx_dict = {col: ascii_uppercase[idx] for idx, col in enumerate(columns)}
    #         start_str_idx = columns_str_idx_dict['Processed_date']
    #         end_str_idx = columns_str_idx_dict['new_location_storage_mp4']
    #         start_index = list(columns).index("Processed_date")
    #         end_index = list(columns).index("new_location_storage_mp4") + 1
    #         columns_in_range = list(columns)[start_index:end_index]
    #         range_name = f'{self.range_name}!{start_str_idx}{row_idx}:{end_str_idx}{row_idx}'
    #         body = {
    #             'values': [[video_info[col] if col in self.required_headers else '' for col in columns_in_range]]}
    #         self.sheets_service.spreadsheets().values().update(
    #             spreadsheetId=self.spreadsheet_id, range=range_name,
    #             valueInputOption='RAW', body=body
    #         ).execute()
    #         print(video_info)
    #         print(body)
    #
    # # Step 6. Upload logs to GCS
    # log_name = f"hs-babyview-upload-log-{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    # storage_client_instance.upload_dict_to_gcs(
    #     data=storage_client_instance.logs, bucket_name="hs-babyview-logs", filename=log_name
    # )


def main():
    # parser = argparse.ArgumentParser(description="Download videos from cloud services")
    # parser.add_argument('--bv_type', type=str, default='main', choices=['main', 'bing', 'luna'],
    #                     help='Babyview Main or Bing')
    # # @TODO: temporarily to run multiple processes for each subject
    # parser.add_argument('--subject_id', type=str, default='all', help='Subject ID to download videos for')
    # args = parser.parse_args()
    process()


if __name__ == '__main__':
    main()
