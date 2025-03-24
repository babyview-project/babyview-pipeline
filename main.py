import argparse
import os
from datetime import datetime
import logging

from moviepy import VideoFileClip

import settings
from controllers import GoogleDriveDownloader
from controllers import FileProcessor
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
        # print(downloading_file_info)
        for video in downloading_file_info:
            # Step 0 Check if video needs to be deleted from buckets.
            if video.status.lower() in ['update', 'delete']:
                delete_success = []
                for gcp_bucket in [f"{video.gcp_bucket_name}_raw", f"{video.gcp_bucket_name}_storage",
                                   f"{video.gcp_bucket_name}_blackout"]:
                    success, delete_msg = storage.delete_blobs_with_substring(bucket_name=gcp_bucket,
                                                                              file_substring=video.unique_video_id)
                    delete_success.append(success)
                    if delete_msg:
                        logs['file_deletion'].append(delete_msg)
                if video.status.lower() == 'delete' and any(d == True for d in delete_success):
                    video.status = 'Removed on GCP'

            # Step 1. Download the raw video file if file id is available
            raw_download_success = False
            if video.google_drive_file_id:
                local_raw_download_path, local_processed_folder = make_local_directory(video)

                raw_download_success, raw_download_error_msg = downloader.download_file(
                    local_raw_download_folder=local_raw_download_path, video=video)
                if raw_download_error_msg:
                    logs['process_raw_fail'].append(
                        f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}_{raw_download_error_msg}')

            else:
                logs['process_raw_fail'].append(
                    f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}_no_drive_file_id')
                local_raw_download_path = None
                local_processed_folder = None
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
                    logs['process_raw_fail'].append(
                        f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}_{raw_upload_error_msg}')
                else:
                    logs['process_raw_sucess'].append(
                        f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}_uploaded_to_{raw_gcp_bucket}/{video.gcp_raw_location}')
            # Step 3. If raw upload success, extract meta, get highlights from go pro.
            print(video.to_dict())
            raw_upload_success = False
            file_processor = FileProcessor(video_raw_path=local_raw_download_path,
                                           processed_folder=local_processed_folder)
            if raw_upload_success and not video.blackout_region:
                # LUNA avi videos do not have meta data, will just compress, but GoPro videos have metadata
                if 'luna' in video.gopro_video_id.lower() or video.gcp_raw_location.lower().endswith('lrv'):
                    video.meta_extract = True
                else:
                    meta_data_output, meta_extract_error_msg = file_processor.extract_meta()
                    print(
                        f"{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id} has meta_data_outputs: {meta_data_output}")
                    if meta_extract_error_msg:
                        logs['meta_extract_fail'].append(
                            f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}_{meta_extract_error_msg}')
                        video.status = 'Meta extraction failed'
                    else:
                        video.meta_extract = True

                    if video.meta_extract:
                        video.highlights, video.device_id, highlights_error_msg = file_processor.get_highlight_and_device_id()
                        if highlights_error_msg:
                            logs['get_highlights_fail'].append(
                                f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}_{meta_extract_error_msg}')
                        if video.highlights:
                            move_highlights_success, move_highlights_msg = storage.move_matching_files(
                                source_bucket_name=f"{video.gcp_bucket_name}_raw",
                                target_bucket_name=f"{video.gcp_bucket_name}_blackout",
                                file_uniq_id=video.unique_video_id)
                            logs['highlights_video_migration'].append(move_highlights_msg)
            # Step 4. Create zip file, compress video and upload it and the video to GCS
            if video.meta_extract and not video.highlights:
                video.compress_video_path, compress_error_msg = file_processor.compress_vid()
                video.zipped_file_path, zipped_file_error_msg = file_processor.zip_files()
                if compress_error_msg or zipped_file_error_msg:
                    logs['compress_zip_fail'].append(
                        f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}_{compress_error_msg}_{zipped_file_error_msg}')
                    video.status = 'Compress Zip Failed'
                else:
                    # upload the zip and mp4 to GCS
                    compress_upload_success, compress_upload_msg = storage.upload_file_to_gcs(
                        source_file_name=video.compress_video_path, destination_path=video.gcp_storage_video_location,
                        gcp_bucket=f"{video.gcp_bucket_name}_storage")
                    zipped_upload_success, zipped_upload_msg = storage.upload_file_to_gcs(
                        source_file_name=video.zipped_file_path, destination_path=video.gcp_storage_zip_location,
                        gcp_bucket=f"{video.gcp_bucket_name}_storage")
                    if compress_upload_msg or zipped_upload_msg:
                        logs['storage_upload_fail'].append(
                            f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}_{compress_upload_msg}_{zipped_upload_msg}')
                        success, delete_msg = storage.delete_blobs_with_substring(
                            bucket_name=f"{video.gcp_bucket_name}_storage",
                            file_substring=video.unique_video_id)
                        video.status = 'Compress Zip Failed'
                    else:
                        try:
                            # get video duration
                            video.duration = VideoFileClip(video.compress_video_path).duration
                            video.close()

                            # remove the downloaded and processed files to save local storage
                            # remove_processed_path = os.path.commonpath([zip_path, video_path])
                            # print(f"Finished processing, removing {remove_processed_path}")
                            # self.clear_directory_contents(remove_processed_path)
                            # remove_raw_path = remove_processed_path.replace('processed', 'raw')
                            # print(f"Finished processing, removing {remove_raw_path}")
                            # self.clear_directory_contents(remove_raw_path)
                            # shutil.rmtree(remove_raw_path)
                        except Exception as e:
                            pass
                        video.status = 'Processed'

            # Step 5. Update the video info with the processed date and duration on the tracking sheet
            video.pipeline_run_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data = {
                'pipeline_run_date': video.pipeline_run_date,
                'status': video.status,
                'duration_sec': video.duration,
                'gcp_raw_location': video.gcp_raw_location,
                'gcp_storage_video_location': video.gcp_storage_video_location,
                'gcp_storage_zip_location': video.gcp_storage_zip_location,
                    }
            airtable.update_video_table_single_video(video_unique_id=video.unique_video_id, data=data)

    print(logs)
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
