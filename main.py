import argparse
import logging
import os
from datetime import datetime
from pathlib import Path
import shutil
from typing import List, Dict, Any
import settings
from controllers import GoogleDriveDownloader, FileProcessor, setup_logging
from imu.utils import process_imu_for_video_dir
from gcp_storage_services import GCPStorageServices
from video import Video
from airtable_services import airtable_services
from status_types import VideoStatus

setup_logging()
logger = logging.getLogger(__name__)

downloader = GoogleDriveDownloader()
storage = GCPStorageServices()


class Step:
    DELETE = "delete"
    DOWNLOAD = "download"
    META = "meta_extract"
    IMU = "imu"
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
    # Store last error on the video so finally() can write it to Airtable if needed
    try:
        video.last_error_msg = str(msg)
        if step in [Step.META, Step.ZIP]:
            video.meta_error_msg = str(msg)
    except Exception:
        pass

    if video.status is None:
        step_status_map = {
            Step.DELETE: VideoStatus.REMOVE_FAIL,
            Step.DOWNLOAD: VideoStatus.DOWNLOAD_FAIL,
            Step.ZIP: VideoStatus.ZIP_FAIL,
            Step.UPLOAD_RAW: VideoStatus.UPLOAD_RAW_FAIL,
            Step.UPLOAD_ZIP: VideoStatus.UPLOAD_ZIP_FAIL,
            Step.UPLOAD_COMPRESS: VideoStatus.UPLOAD_COMPRESS_FAIL,
        }
        video.status = step_status_map.get(step, video.status)

    result = {
        "video_id": video.unique_video_id,
        "subject_id": video.subject_id,
        "step": step,
        "message": msg,
    }
    logger.error(
        "step_failed video_id=%s subject_id=%s step=%s message=%s",
        video.unique_video_id,
        video.subject_id,
        step,
        msg,
    )
    logs[f'{step}_fail'].append(result)
    return False


def handle_deletion(video, logs):
    delete_ok = True
    for gcp_bucket in [f"{video.gcp_bucket_name}_raw", f"{video.gcp_bucket_name}_storage"]:
        success, msg = storage.delete_blobs_with_substring(gcp_bucket, video.unique_video_id)
        if msg:
            logger.warning(
                "delete_msg video_id=%s bucket=%s message=%s",
                video.unique_video_id,
                gcp_bucket,
                msg,
            )
            logs['file_deletion'].append(msg)
        delete_ok &= success

    if not delete_ok:
        video.status = VideoStatus.REMOVE_FAIL
        return fail_step(logs, video, Step.DELETE, "Some files failed deletion")

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
        video.gcp_storage_zip_location = meta_err
        fail_step(logs, video, Step.META, meta_err)
        return True  # still continue even on meta failure

    # video.highlight, hi_err = processor.highlight_detection()
    # if hi_err:
    #     return fail_step(logs, video, Step.HIGHLIGHT, hi_err)
    #
    # if video.highlight:
    #     logs['highlight_detected'].append(video.unique_video_id)

    return True


def process_imu(video, logs):
    metadata_dir = os.path.join(video.local_processed_folder, f"{video.gcp_file_name}_metadata")
    try:
        imu_df = process_imu_for_video_dir(metadata_dir)
        if imu_df is None:
            return fail_step(logs, video, Step.IMU, f"IMU txt files missing in {metadata_dir}")
        video.comment = imu_df.attrs.get("comment")
        imu_csv_path = os.path.join(metadata_dir, "imu_combined.csv")
        imu_df.to_csv(imu_csv_path, index=False)
        return True
    except Exception as e:
        return fail_step(logs, video, Step.IMU, e)


def upload_raw(video, logs):
    bucket = f"{video.gcp_bucket_name}_raw"
    success, msg = storage.upload_file_to_gcs(video.local_raw_download_path, video.gcp_raw_location, bucket)
    if msg:
        return fail_step(logs, video, Step.UPLOAD_RAW, msg)
    print(f'Uploading: {video.unique_video_id} to {bucket}/{video.gcp_raw_location}')
    logs['process_raw_success'].append(f'{video.unique_video_id} to {bucket}/{video.gcp_raw_location}')
    return True


def zip_metadata(
    video,
    processor,
    logs,
    *,
    min_zip_kb: float = 3.0,
    max_attempts: int = 3,
    add_imu_suffix: bool = False,
):
    last_zip_kb = None

    for attempt in range(1, max_attempts + 1):
        # On retry: re-run meta extraction to regenerate metadata folder
        if attempt > 1:
            meta_dir = os.path.join(video.local_processed_folder, f"{video.gcp_file_name}_metadata")
            try:
                shutil.rmtree(meta_dir)
            except Exception:
                pass
            os.makedirs(meta_dir, exist_ok=True)

            _, meta_err = processor.extract_meta()
            if meta_err:
                video.status = VideoStatus.META_FAIL
                fail_step(logs, video, Step.META, meta_err)
                return True  # stop later, but keep Airtable update

        # (Re)zip metadata
        video.zipped_file_path, zip_err = processor.zip_files()
        if zip_err:
            return fail_step(logs, video, Step.ZIP, zip_err)

        try:
            last_zip_kb = os.path.getsize(video.zipped_file_path) / 1024.0
        except Exception:
            last_zip_kb = 0.0

        if last_zip_kb >= min_zip_kb:
            break  # good zip

        # Too small => retry if we still can
        if attempt < max_attempts:
            try:
                os.remove(video.zipped_file_path)
            except Exception:
                pass
            continue

        # Still too small after max attempts => fatal META_FAIL, no upload
        video.status = VideoStatus.META_FAIL
        fail_step(
            logs,
            video,
            Step.ZIP,
            f"Metadata zip too small: {last_zip_kb:.3f} KB (< {min_zip_kb} KB) after {max_attempts} attempts."
        )
        return True

    # Optionally rename zip if IMU succeeded
    if add_imu_suffix:
        base, ext = os.path.splitext(video.zipped_file_path)
        if not base.endswith("_imu"):
            renamed_path = f"{base}_imu{ext}"
            try:
                os.rename(video.zipped_file_path, renamed_path)
                video.zipped_file_path = renamed_path
            except Exception as e:
                return fail_step(logs, video, Step.ZIP, f"Failed to rename zip with _imu suffix: {e}")

    # Normal path: upload zip
    video.gcp_storage_zip_location = f"{video.subject_id}/{os.path.basename(video.zipped_file_path)}"
    _, zip_upload_msg = storage.upload_file_to_gcs(
        video.zipped_file_path,
        video.gcp_storage_zip_location,
        f"{video.gcp_bucket_name}_storage"
    )
    if zip_upload_msg:
        return fail_step(logs, video, Step.UPLOAD_ZIP, zip_upload_msg)

    return True


def compress_rotate_blackout(video: Video, processor, logs):
    video.compress_video_path, compress_err = processor.compress_vid()
    if compress_err:
        video.status = VideoStatus.COMPRESS_FAIL
        return fail_step(logs, video, Step.COMPRESS, compress_err)

    video.compress_video_path, rotate_err = processor.rotate_video()
    if rotate_err:
        video.status = VideoStatus.ROTATE_FAIL
        return fail_step(logs, video, Step.ROTATE, rotate_err)
    if video.blackout_region:
        video.compress_video_path, blackout_err = processor.blackout_video()
        if blackout_err:
            video.status = VideoStatus.BLACKOUT_FAIL
            return fail_step(logs, video, Step.BLACKOUT, blackout_err)

    return True


def compressed_upload(video: Video, logs):
    video.gcp_storage_video_location = f"{video.subject_id}/{os.path.basename(video.compress_video_path)}"
    _, compress_upload_msg = storage.upload_file_to_gcs(
        video.compress_video_path,
        video.gcp_storage_video_location,
        f"{video.gcp_bucket_name}_storage")
    if compress_upload_msg:
        return fail_step(logs, video, Step.UPLOAD_COMPRESS, compress_upload_msg)

    logs['storage_upload_success'].append(f'{video.unique_video_id} uploaded to {video.gcp_storage_video_location}')
    return True


def process_single_video(video: Video, logs):
    processor = FileProcessor(video)
    imu_failed = False

    try:
        # Step 1:
        # If status == delete, delete orig files and mark airtable
        # If status == reprocess, delete orig files and continue processing
        if video.status and video.status in [VideoStatus.TO_BE_DELETED, VideoStatus.TO_BE_REPROCESS]:
            result = handle_deletion(video, logs)

            if video.status == VideoStatus.TO_BE_DELETED:
                video.status = VideoStatus.REMOVED
                return

            if result is False:
                return
        # Step 2:
        # Download the video from Google Drive
        video.status = None
        if not download_video(video, processor, logs):
            return

        # Step 3:
        # Extract meta data from video, upload raw to bucket
        if not process_metadata(video=video, processor=processor, logs=logs):
            return

        meta_failed = video.status == VideoStatus.META_FAIL
        if 'luna' in video.gopro_video_id.lower():
            imu_failed = False
            video.comment = None
        else:
            imu_failed = not process_imu(video, logs)
        if not upload_raw(video, logs):
            return
        if meta_failed:
            return  # ensure stop after raw upload if metadata failed

        # Step 4:
        # Zip and compress the meta data and vid, upload to storage bucket
        if 'luna' not in video.gopro_video_id.lower() and not video.gcp_raw_location.lower().endswith('lrv'):
            if not zip_metadata(video, processor, logs, add_imu_suffix=not imu_failed):
                return
            if video.status in [VideoStatus.META_FAIL]:
                return

        if not compress_rotate_blackout(video, processor, logs):
            return

        if not compressed_upload(video, logs):
            return

        # Fetch GCS object sizes after uploads
        video.video_size_mb, video.metadata_size_kb, size_err = storage.get_object_sizes(
            f"{video.gcp_bucket_name}_storage",
            video.gcp_storage_video_location,
            video.gcp_storage_zip_location,
        )
        if size_err:
            logs.setdefault('gcs_size_check_failed', []).append(
                f"{video.unique_video_id}: {size_err}"
            )
            logger.warning("gcs_size_check_failed video_id=%s error=%s", video.unique_video_id, size_err)

        if not video.status:
            video.status = VideoStatus.PROCESSED

    except Exception as e:
        video.status = VideoStatus.UNEXPECTED_FAIL
        logs['unexpected_error'].append(f'{video.unique_video_id}_{str(e)}')
        logger.exception("unexpected_error video_id=%s error=%s", video.unique_video_id, e)

    finally:
        zip_field_value = None
        if video.status in [VideoStatus.META_FAIL, VideoStatus.ZIP_FAIL]:
            zip_field_value = getattr(video, "meta_error_msg", None) or getattr(video, "last_error_msg", None)
        else:
            zip_field_value = (
                f'{video.gcp_bucket_name}_storage/{video.gcp_storage_zip_location}'
                if video.gcp_storage_zip_location else None
            )

        video.pipeline_run_date = datetime.now().strftime("%Y-%m-%d")
        airtable_services.update_video_table_single_video(video.unique_video_id, {
            # 'hilight_locations': str(video.highlight) if video.highlight else None,
            'pipeline_run_date': video.pipeline_run_date,
            'status': video.status,
            'duration_sec': video.duration if video.duration else None,
            'gcp_raw_location': f'{video.gcp_bucket_name}_raw/{video.gcp_raw_location}' if video.local_raw_download_path else None,
            'gcp_storage_video_location': f'{video.gcp_bucket_name}_storage/{video.gcp_storage_video_location}' if video.gcp_storage_video_location else None,
            'gcp_storage_zip_location': zip_field_value,
            'video_size_mb': getattr(video, "video_size_mb", None),
            'metadata_size_kb': getattr(video, "metadata_size_kb", None),
            'comment': getattr(video, "last_error_msg", None) if imu_failed else getattr(video, "comment", None),
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
    downloading_file_info, log_message = downloader.get_file_paths_from_google_drive(
        video_info_from_tracking=video_tracking_data)
    logs['loading_download_info_error'].append(log_message)

    for video in downloading_file_info:
        try:
            process_single_video(video, logs)
        except Exception as e:
            logs['general_error'].append({f'{video.unique_video_id}': str(e)})

    log_name = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_logs.json"
    storage.upload_dict_to_gcs(dict(logs), "hs-babyview-logs", log_name)
    logger.info("logs_uploaded bucket=hs-babyview-logs object=%s", log_name)

    return dict(logs)


def main():
    parser = argparse.ArgumentParser(description="Download videos from cloud services")
    filter_group = parser.add_mutually_exclusive_group()
    filter_group.add_argument(
        '--pipeline_run_date',
        nargs='?',
        const='__NULL__',
        default=None,
        help="YYYY-MM-DD or provide no value to select NULL pipeline_run_date",
    )
    filter_group.add_argument('--status', nargs='+', default=None, help="One or more statuses")
    filter_group.add_argument(
        '--dataset',
        nargs='+',
        default=None,
        choices=['BV-main', 'Luna', 'Bing'],
        help="One or more datasets",
    )
    filter_group.add_argument('--subject_id', nargs='+', default=None, help="One or more subject IDs")
    filter_group.add_argument('--unique_video_id', nargs='+', default=None, help="One or more video record IDs")
    filter_group.add_argument('--status_test', type=str, default=None, help="Exact status_test value")
    filter_group.add_argument('--release', type=str, default=None, help="Release name from Releases table")
    parser.add_argument('--dry_run', action='store_true', help="Only load Airtable and print count")
    parser.add_argument('--limit', type=int, default=None, help="Optional max number of videos to process")
    parser.add_argument(
        '--no_base_filter',
        action='store_true',
        help="Bypass base Airtable filters (status/logging_date)",
    )

    args = parser.parse_args()

    process_filter_key = None
    process_filter_value = None

    if args.release:
        process_filter_key = "release"
        process_filter_value = args.release
    elif args.subject_id:
        process_filter_key = "subject_id"
        process_filter_value = args.subject_id
    elif args.unique_video_id:
        process_filter_key = "unique_video_id"
        process_filter_value = args.unique_video_id
    elif args.status:
        process_filter_key = "status"
        process_filter_value = args.status
    elif args.dataset:
        process_filter_key = "dataset"
        process_filter_value = args.dataset
    elif args.status_test:
        process_filter_key = "status_test"
        process_filter_value = args.status_test
    elif args.pipeline_run_date is not None:
        process_filter_key = "pipeline_run_date"
        process_filter_value = (
            None if args.pipeline_run_date == "__NULL__" else args.pipeline_run_date
        )

    include_base_filters = not args.no_base_filter
    exclude_meta_fail = process_filter_key not in ["status_test", "unique_video_id"]

    if process_filter_key == "release" and process_filter_value:
        release_name = process_filter_value[0] if isinstance(process_filter_value, list) else process_filter_value
        video_ids = airtable_services.get_video_ids_for_a_release_set(release_name)
        video_tracking_data = airtable_services.get_video_info_by_record_ids(
            video_ids,
            limit=args.limit,
            include_base_filters=include_base_filters,
            exclude_meta_fail=exclude_meta_fail,
        )
    elif process_filter_key == "subject_id" and process_filter_value:
        subject_ids = process_filter_value if isinstance(process_filter_value, list) else [process_filter_value]
        video_tracking_data = airtable_services.get_video_info_for_subject_ids(
            subject_ids,
            limit=args.limit,
            include_base_filters=include_base_filters,
            exclude_meta_fail=exclude_meta_fail,
        )
    else:
        video_tracking_data = airtable_services.get_video_info_from_video_table(
            filter_key=process_filter_key,
            filter_value=process_filter_value,
            limit=args.limit,
            include_base_filters=include_base_filters,
            exclude_meta_fail=exclude_meta_fail,
        )
    logger.info("airtable_loaded count=%s", len(video_tracking_data))
    if args.dry_run:
        print(f"[DRY_RUN] Loaded {len(video_tracking_data)} videos from Airtable.")
        return
    process_videos(video_tracking_data=video_tracking_data)



if __name__ == '__main__':
    main()
