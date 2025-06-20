import argparse
import os
from datetime import datetime
from pathlib import Path

import settings
from controllers import GoogleDriveDownloader
from controllers import FileProcessor
from gcp_storage_services import GCPStorageServices
from video import Video
from airtable_services import airtable_services
downloader = GoogleDriveDownloader()
storage = GCPStorageServices()


class VideoStatus:
    TO_BE_DELETED = "to_be_deleted"
    TO_BE_REPROCESS = "to_be_reprocessed"

    REMOVED = "successfully_deleted_from_GCP"
    PROCESSED = "successfully_processed"

    META_FAIL = "error_in_meta_extraction"
    REMOVE_FAIL = "error_in_GCP_deletion"

    NOT_FOUND = "not_found"


class Step:
    DELETE = "delete"
    DOWNLOAD = "download"
    META = "meta_extract"
    # HIGHLIGHT = "get_highlights"
    ZIP = "zip"
    COMPRESS = "compress"
    ROTATE = 'rotate'
    BLACKOUT = 'blackout'
    UPLOAD_ZIP = "upload_zip"
    UPLOAD_COMPRESS = "upload_compress"
    UPLOAD_RAW = "upload_raw"


def make_local_directory(video):
    os.makedirs(settings.raw_file_root, exist_ok=True)
    os.makedirs(settings.process_file_root, exist_ok=True)

    entry_point = settings.google_drive_entry_point_folder_names[1] if 'bing' in video.dataset.lower() \
        else settings.google_drive_entry_point_folder_names[0]

    local_raw_download_path = os.path.join(settings.raw_file_root, entry_point, video.gcp_raw_location)
    local_raw_download_folder = os.path.dirname(local_raw_download_path)
    local_processed_folder = os.path.join(settings.process_file_root, entry_point, video.subject_id,
                                          video.gopro_video_id)
    local_processed_meta_data_folder = os.path.join(local_processed_folder, f'{video.gcp_file_name}_metadata')

    for folder in [local_raw_download_folder, local_processed_folder, local_processed_meta_data_folder]:
        os.makedirs(folder, exist_ok=True)

    return Path(local_raw_download_path).resolve(), Path(local_processed_folder).resolve()


def fail_step(logs, video, step, msg):
    result = {
        "video_id": video.unique_video_id,
        "subject_id": video.subject_id,
        "step": step,
        "message": msg,
    }
    print(result)
    logs[f'{step}_fail'].append(result)
    return False


def handle_deletion(video, logs):
    delete_ok = True
    for gcp_bucket in [f"{video.gcp_bucket_name}_raw", f"{video.gcp_bucket_name}_storage"]:
        success, msg = storage.delete_blobs_with_substring(gcp_bucket, video.unique_video_id)
        if msg:
            print(f'Deletion msg: {msg}')
            logs['file_deletion'].append(msg)
        delete_ok &= success

    if not delete_ok:
        video.status = VideoStatus.REMOVE_FAIL
        return fail_step(logs, video, Step.DELETE, "Some files failed deletion")

    if video.status == VideoStatus.TO_BE_DELETED:
        video.status = VideoStatus.REMOVED
        return False

    return True


def download_video(video, processor, logs):
    if not video.google_drive_file_id:
        video.status = VideoStatus.NOT_FOUND
        return False

    video.local_raw_download_path, video.local_processed_folder = make_local_directory(video)
    success, msg = downloader.download_file(video.local_raw_download_path, video)
    if msg:
        return fail_step(logs, video, Step.DOWNLOAD, msg)

    video.duration = processor.get_video_duration()

    return True


def process_metadata(video, processor, logs):
    if 'luna' in video.gopro_video_id.lower() or video.gcp_raw_location.lower().endswith('lrv'):
        return True

    meta_data_output, meta_err = processor.extract_meta()
    if meta_err:
        video.status = VideoStatus.META_FAIL
        fail_step(logs, video, Step.META, meta_err)
        return True  # still continue even on meta failure

    # video.highlight, hi_err = processor.highlight_detection()
    # if hi_err:
    #     return fail_step(logs, video, Step.HIGHLIGHT, hi_err)
    #
    # if video.highlight:
    #     logs['highlight_detected'].append(video.unique_video_id)

    return True


def upload_raw(video, logs):
    bucket = f"{video.gcp_bucket_name}_raw"
    success, msg = storage.upload_file_to_gcs(video.local_raw_download_path, video.gcp_raw_location, bucket)
    if msg:
        return fail_step(logs, video, Step.UPLOAD_RAW, msg)
    print(f'Uploading: {video.unique_video_id} to {bucket}/{video.gcp_raw_location}')
    logs['process_raw_success'].append(f'{video.unique_video_id} to {bucket}/{video.gcp_raw_location}')
    return True


def zip_metadata(video, processor, logs):
    video.zipped_file_path, zip_err = processor.zip_files()
    if zip_err:
        return fail_step(logs, video, Step.ZIP, zip_err)

    video.gcp_storage_zip_location = f"{video.subject_id}/{os.path.basename(video.zipped_file_path)}"
    _, zip_upload_msg = storage.upload_file_to_gcs(
        video.zipped_file_path,
        video.gcp_storage_zip_location,
        f"{video.gcp_bucket_name}_storage")
    if zip_upload_msg:
        return fail_step(logs, video, Step.UPLOAD_ZIP, zip_upload_msg)

    return True


def compress_rotate_blackout_and_upload(video: Video, processor, logs):
    video.compress_video_path, compress_err = processor.compress_vid()
    if compress_err:
        return fail_step(logs, video, Step.COMPRESS, compress_err)

    video.compress_video_path, rotate_err = processor.rotate_video()
    if rotate_err:
        return fail_step(logs, video, Step.ROTATE, rotate_err)
    if video.blackout_region:
        video.compress_video_path, blackout_err = processor.blackout_video()
        if blackout_err:
            return fail_step(logs, video, Step.BLACKOUT, blackout_err)

    video.gcp_storage_video_location = f"{video.subject_id}/{os.path.basename(video.compress_video_path)}"
    _, compress_upload_msg = storage.upload_file_to_gcs(
        video.compress_video_path,
        video.gcp_storage_video_location,
        f"{video.gcp_bucket_name}_storage")
    if compress_upload_msg:
        return fail_step(logs, video, Step.UPLOAD_COMPRESS, compress_upload_msg)

    logs['storage_upload_success'].append(f'{video.unique_video_id} uploaded to {video.gcp_storage_video_location}')
    video.status = VideoStatus.PROCESSED
    return True


def process_single_video(video: Video, logs):
    processor = FileProcessor(video)
    error_occurred = False

    try:
        # Step 1:
        # If status == delete, delete orig files and mark airtable
        # If status == reprocess, delete orig files and continue processing
        if video.status and video.status in [VideoStatus.TO_BE_DELETED, VideoStatus.TO_BE_REPROCESS]:
            result = handle_deletion(video, logs)
            if result is False:
                return
        # Step 2:
        # Download the video from Google Drive
        if not download_video(video, processor, logs):
            if not video.google_drive_file_id:
                error_occurred = False
            else:
                error_occurred = True
            return

        # Step 3:
        # Extract meta data from video, upload raw to bucket
        if not process_metadata(video=video, processor=processor, logs=logs):
            return
        if video.status in [VideoStatus.META_FAIL]:
            if not upload_raw(video, logs):
                error_occurred = True
            return  # ensure stop after raw upload in these cases
        else:
            if not upload_raw(video, logs):
                error_occurred = True
                return

        # Step 4:
        # Zip and compress the meta data and vid, upload to storage bucket
        if 'luna' not in video.gopro_video_id.lower() and not video.gcp_raw_location.lower().endswith('lrv'):
            if not zip_metadata(video, processor, logs):
                error_occurred = True
                return

        if not compress_rotate_blackout_and_upload(video, processor, logs):
            error_occurred = True
            return

        video.status = VideoStatus.PROCESSED

    except Exception as e:
        error_occurred = True
        logs['unexpected_error'].append(f'{video.unique_video_id}_{str(e)}')

    finally:
        if not error_occurred:
            video.pipeline_run_date = datetime.now().strftime("%Y-%m-%d")
            airtable_services.update_video_table_single_video(video.unique_video_id, {
                # 'hilight_locations': str(video.highlight) if video.highlight else None,
                'pipeline_run_date': video.pipeline_run_date,
                'status': video.status,
                'duration_sec': video.duration if video.duration else None,
                'gcp_raw_location': f'{video.gcp_bucket_name}_raw/{video.gcp_raw_location}' if video.local_raw_download_path else None,
                'gcp_storage_video_location': f'{video.gcp_bucket_name}_storage/{video.gcp_storage_video_location}' if video.gcp_storage_video_location else None,
                'gcp_storage_zip_location': f'{video.gcp_bucket_name}_storage/{video.gcp_storage_zip_location}' if video.gcp_storage_zip_location else None,
            })
        if video.google_drive_file_id:
            processor.clear_directory_contents_raw_storage()


def process_videos(video_tracking_data):
    from collections import defaultdict
    logs = defaultdict(list)

    storage.check_gcs_buckets()

    if video_tracking_data.empty:
        logs['airtable'].append("No_Record_From_Airtable.")
        return dict(logs)

    logs['airtable'].append(f"{len(video_tracking_data)}_Loaded")
    downloading_file_info, log_message = downloader.get_downloading_file_paths(
        video_info_from_tracking=video_tracking_data)
    logs['loading_download_info_error'].append(log_message)

    for video in downloading_file_info:
        process_single_video(video, logs)

    log_name = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_logs.json"
    storage.upload_dict_to_gcs(dict(logs), "hs-babyview-logs", log_name)

    return dict(logs)


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

    video_tracking_data = airtable_services.get_video_info_from_video_table(filter_key=filter_key, filter_value=filter_value)
    print(video_tracking_data, len(video_tracking_data))
    process_videos(video_tracking_data=video_tracking_data)

    # from video import Video
    # v = Video({'date': '2024-01-01', })
    # local_raw_download_path = os.path.join(settings.raw_file_root, 'BabyView_Main', 'S00400001_S00400001_2023-06-12_1_recnH3x6JqlT6LsN3.mp4')
    # v.compress_video_path = local_raw_download_path # Path(local_raw_download_path).resolve()
    # file_processor = FileProcessor(video=v)
    # path, error_msg = file_processor.rotate_video()
    # print(path, error_msg)

    # from video import Video
    # v = Video({'date': '2024-01-01', })
    # from collections import defaultdict
    # logs = defaultdict(list)
    #
    # local_raw_download_path = os.path.join('data', 'HERO_Stabilized_1080.mp4')
    # v.local_processed_folder = os.path.join('data', 'Bones')
    # v.gcp_file_name = 'HERO_Stabilized_1080'
    # v.local_raw_download_path = Path(local_raw_download_path).resolve()
    # v.gcp_raw_location = 'asdf'
    # v.gopro_video_id = 'asdf'
    # processor = FileProcessor(v)
    # r = process_metadata(video=v, processor=processor, logs=logs)
    # print(r, logs)


if __name__ == '__main__':
    main()
