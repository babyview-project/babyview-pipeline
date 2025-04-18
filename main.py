import argparse
import os
from datetime import datetime
from pathlib import Path

import settings
from controllers import GoogleDriveDownloader
from controllers import FileProcessor
from airtable_services import AirtableServices
from gcp_storage_services import GCPStorageServices


airtable = AirtableServices()
downloader = GoogleDriveDownloader()
storage = GCPStorageServices()


def make_local_directory(video):
    if not os.path.exists(settings.raw_file_root):
        os.makedirs(settings.raw_file_root, exist_ok=True)
    if not os.path.exists(settings.process_file_root):
        os.makedirs(settings.process_file_root, exist_ok=True)

    entry_point = settings.google_drive_entry_point_folder_names[1] if 'bing' in video.dataset.lower() else \
        settings.google_drive_entry_point_folder_names[0]
    local_raw_download_path = os.path.join(settings.raw_file_root, entry_point, video.gcp_raw_location)
    local_raw_download_folder = os.path.dirname(local_raw_download_path)
    local_processed_folder = os.path.join(settings.process_file_root, entry_point, video.subject_id,
                                          video.gopro_video_id)
    local_processed_meta_data_folder = os.path.join(settings.process_file_root, entry_point, video.subject_id,
                                                    video.gopro_video_id, f'{video.gcp_file_name}_metadata')
    local_processed_highlights_device_info_folder = os.path.join(settings.process_file_root, entry_point,
                                                                 video.subject_id,
                                                                 video.gopro_video_id, 'highlights_device_info')

    if not os.path.exists(local_raw_download_folder):
        os.makedirs(local_raw_download_folder, exist_ok=True)

    if not os.path.exists(local_processed_folder):
        os.makedirs(local_processed_folder, exist_ok=True)

    if not os.path.exists(local_processed_highlights_device_info_folder):
        os.makedirs(local_processed_highlights_device_info_folder, exist_ok=True)

    if not os.path.exists(local_processed_meta_data_folder):
        os.makedirs(local_processed_meta_data_folder, exist_ok=True)

    return Path(local_raw_download_path).resolve(), Path(local_processed_folder).resolve()


def process(video_tracking_data):
    logs = {
        'process_raw_success': [],
        'process_raw_fail': [],
        'storage_upload_success': [],
        'storage_upload_fail': [],
        'meta_extract_fail': [],
        'get_highlights_fail': [],
        'compress_zip_fail': [],
        'highlights_video_migration': [],
        'gopro_highlight_detected': [],
        'file_deletion': [],
    }

    storage.check_gcs_buckets()
    if video_tracking_data.empty:
        logs['airtable'] = "No_Record_From_Airtable."
    else:
        logs['airtable'] = f"{len(video_tracking_data)}_Loaded"

        downloading_file_info, downloading_file_info_log = downloader.get_downloading_file_paths(
            video_info_from_tracking=video_tracking_data)
        logs['loading_download_info_error'] = downloading_file_info_log
        for video in downloading_file_info:
            raw_download_success = False
            raw_upload_success = False
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
                    video.status = 'Removed from GCP'

            # Step 1. Download the raw video file if file id is available
            if video.google_drive_file_id and video.status.lower() != 'delete':
                video.local_raw_download_path, video.local_processed_folder = make_local_directory(video)

                raw_download_success, raw_download_error_msg = downloader.download_file(
                    local_raw_download_folder=video.local_raw_download_path, video=video)
                if raw_download_error_msg:
                    logs['process_raw_fail'].append(
                        f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}_{raw_download_error_msg}')

            else:
                logs['process_raw_fail'].append(
                    f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}_no_drive_file_id')
                video.status = 'Not found'

            # Step 2. Upload raw video file to GCS if download is successful.
            if raw_download_success and not raw_upload_success:
                if video.blackout_region:
                    raw_gcp_bucket = f"{video.gcp_bucket_name}_blackout"
                    video.status = 'To blackout'
                else:
                    raw_gcp_bucket = f"{video.gcp_bucket_name}_raw"
                print(f"Uploading {video.gcp_file_name} to {raw_gcp_bucket}/{video.gcp_raw_location}")

                raw_upload_success, raw_upload_error_msg = storage.upload_file_to_gcs(
                    source_file_name=video.local_raw_download_path,
                    destination_path=video.gcp_raw_location,
                    gcp_bucket=raw_gcp_bucket)
                # raw_upload_msg, raw_upload_success = "Test", True
                if raw_upload_error_msg:
                    logs['process_raw_fail'].append(
                        f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}_{raw_upload_error_msg}')
                else:
                    logs['process_raw_success'].append(
                        f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}_uploaded_to_{raw_gcp_bucket}/{video.gcp_raw_location}')
            # Step 3. If raw upload success, extract meta, get highlights from go pro.
            file_processor = FileProcessor(video=video)
            video.duration = file_processor.get_video_duration()
            if raw_upload_success and not video.blackout_region:
                # LUNA avi videos do not have meta data, will just compress, but GoPro videos have metadata
                if 'luna' in video.gopro_video_id.lower() or video.gcp_raw_location.lower().endswith('lrv'):
                    video.meta_extract = True
                else:
                    meta_data_output, meta_extract_error_msg = file_processor.extract_meta()
                    if meta_extract_error_msg:
                        print(meta_extract_error_msg)
                        logs['meta_extract_fail'].append(
                            f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}_{meta_extract_error_msg}')
                        video.status = 'Meta extraction failed'
                    else:
                        video.meta_extract = True

                    if video.meta_extract:
                        video.highlight, highlights_error_msg = file_processor.highlight_detection()
                        if highlights_error_msg:
                            print(highlights_error_msg)
                            logs['get_highlights_fail'].append(
                                f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}_{highlights_error_msg}')
                        if video.highlight:
                            move_highlights_success, move_highlights_msg = storage.move_matching_files(
                                source_bucket_name=f"{video.gcp_bucket_name}_raw",
                                target_bucket_name=f"{video.gcp_bucket_name}_blackout",
                                file_uniq_id=video.unique_video_id)
                            logs['gopro_highlight_detected'].append(move_highlights_msg)
                            video.status = 'Gopro highlight detected'
            # Step 4. Create zip file, compress video and upload it and the video to GCS
            if video.meta_extract and not video.highlight:
                # Zip non-luna meta and upload to GCS
                zipped_file_error_msg = None
                if 'luna' not in video.gopro_video_id.lower():
                    video.zipped_file_path, zipped_file_error_msg = file_processor.zip_files()
                    if zipped_file_error_msg:
                        print(f'{video.unique_video_id}_{zipped_file_error_msg}')
                        logs['compress_zip_fail'].append(
                            f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}: zip_failed_{zipped_file_error_msg}')
                        video.status = 'Compress zip failed'
                    else:
                        video.gcp_storage_zip_location = f"{video.subject_id}/{os.path.basename(video.zipped_file_path)}"
                        zipped_upload_success, zipped_upload_msg = storage.upload_file_to_gcs(
                            source_file_name=video.zipped_file_path, destination_path=video.gcp_storage_zip_location,
                            gcp_bucket=f"{video.gcp_bucket_name}_storage")
                        if zipped_upload_msg:
                            print(f'{video.unique_video_id}_{zipped_upload_msg}')
                            logs['storage_upload_fail'].append(
                                f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}: zip_upload_fail_{zipped_upload_msg}')
                            video.status = 'Compress zip failed'
                # Compress all vids and upload to GCS
                video.compress_video_path, compress_error_msg = file_processor.compress_vid()
                if compress_error_msg:
                    print(f'{video.unique_video_id}_{compress_error_msg}')
                    logs['compress_zip_fail'].append(
                        f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}: compress_fail_{compress_error_msg}')
                    video.status = 'Compress zip failed'
                else:
                    video.gcp_storage_video_location = f"{video.subject_id}/{os.path.basename(video.compress_video_path)}"
                    compress_upload_success, compress_upload_msg = storage.upload_file_to_gcs(
                        source_file_name=video.compress_video_path,
                        destination_path=video.gcp_storage_video_location,
                        gcp_bucket=f"{video.gcp_bucket_name}_storage")
                    if compress_upload_msg:
                        print(f'{video.unique_video_id}_{compress_upload_msg}')
                        logs['storage_upload_fail'].append(
                            f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}: compress_upload_fail_{compress_upload_msg}')
                        video.status = 'Compress zip failed'

                if compress_error_msg or zipped_file_error_msg:
                    success, delete_msg = storage.delete_blobs_with_substring(
                        bucket_name=f"{video.gcp_bucket_name}_storage",
                        file_substring=video.unique_video_id)
                    video.status = 'Compress zip failed'
                    if delete_msg:
                        logs['file_deletion'].append(delete_msg)
                else:
                    print(
                        f"Uploaded {video.gcp_file_name} to {video.gcp_bucket_name}_storage/{video.gcp_storage_video_location} and {video.gcp_storage_zip_location}")
                    logs['storage_upload_success'].append(
                        f'{video.unique_video_id}_{video.subject_id}_{video.gopro_video_id}')
                    # video.duration = file_processor.get_compressed_video_duration(
                    #     compressed_video_path=video.compress_video_path)
                    video.status = 'Processed'

            file_processor.clear_directory_contents_raw_storage()
            print(video.to_dict())
            # Step 5. Update the video info with the processed date and duration on the tracking sheet
            video.pipeline_run_date = datetime.now().strftime("%Y-%m-%d")
            data = {
                'pipeline_run_date': video.pipeline_run_date,
                'status': video.status,
                'duration_sec': video.duration,
                'gcp_raw_location': f'{video.gcp_bucket_name}_raw/{video.gcp_raw_location}',
                'gcp_storage_video_location': f'{video.gcp_bucket_name}_storage/{video.gcp_storage_video_location}' if video.gcp_storage_video_location else None,
                'gcp_storage_zip_location': f'{video.gcp_bucket_name}_storage/{video.gcp_storage_zip_location}' if video.gcp_storage_zip_location else None,
            }
            airtable.update_video_table_single_video(video_unique_id=video.unique_video_id, data=data)

    # print(logs)
    # Step 6. Upload logs to GCS
    log_name = f"hs-babyview-upload-log-{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    storage.upload_dict_to_gcs(
        data=logs, bucket_name="hs-babyview-logs", filename=log_name
    )


def main():
    parser = argparse.ArgumentParser(description="Download videos from cloud services")
    parser.add_argument('--filter_key', type=str, default='pipeline_run_date',  #None
                        choices=['pipeline_run_date', 'status', 'dataset', 'subject_id', 'unique_video_id'],
                        help="Choose from ['pipeline_run_date', 'status', 'dataset', 'subject_id', 'unique_video_id']")
    parser.add_argument('--filter_value', type=str, default=None,
                        help="Choose the value for the filter_key")

    args = parser.parse_args()

    if settings.forced_filter:
        filter_key = settings.forced_filter_key
        filter_value = settings.forced_filter_value
    else:
        filter_key = args.filter_key
        filter_value = args.filter_value

    video_tracking_data = airtable.get_video_info_from_video_table(filter_key=filter_key, filter_value=filter_value)
    print(video_tracking_data, len(video_tracking_data))
    process(video_tracking_data=video_tracking_data)
    # from video import Video
    # v = Video({'date': '2024-01-01'})
    # local_raw_download_path = os.path.join(settings.raw_file_root, 'BabyView_Main', 'S01420001_rec0NwEqXa9gYtbyX.MP4')
    # v.local_raw_download_path = Path(local_raw_download_path).resolve()
    # file_processor = FileProcessor(video=v)
    # h, m = file_processor.highlight_detection()
    # print(h, m)


if __name__ == '__main__':
    main()
