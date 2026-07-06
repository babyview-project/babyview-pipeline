"""
Batch-compress videos that failed metadata extraction but have raw files in GCS.

Query: status = error_in_meta_extraction, gcp_storage_video_location empty, gcp_raw_location set.

For each row:
  - download raw from gcp_raw_location
  - compress, rotate, and blackout (same steps as main pipeline)
  - upload to the storage bucket
  - update gcp_storage_video_location, video_size_mb, pipeline_run_date, status_test on success
  - on compression failure: set status to error_in_compression, store error in gcp_storage_video_location
  - leave status unchanged on other failures (status_test only)

Usage:
  python compress_meta_fail_batch.py --dry_run --limit 5
  python compress_meta_fail_batch.py --limit 20
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
from datetime import datetime

import pytz
from tqdm import tqdm

from controllers import FileProcessor, setup_logging, compress_rotate_blackout_video
from gcp_storage_services import GCPStorageServices
from airtable_services import airtable_services
from video import Video
from status_types import VideoStatus

setup_logging()
logger = logging.getLogger(__name__)

STATUS_TEST_SUCCESS = "compressed_batch_sucess"
STATUS_TEST_MAX_LEN = 1000
WORK_ROOT = os.path.join("data", "bv_tmp", "compress_meta_fail_batch")

storage = GCPStorageServices()


def _today_pipeline_run_date() -> str:
    tz = pytz.timezone("America/Los_Angeles")
    return datetime.now(tz).strftime("%Y-%m-%d")


def _parse_gcs_location(location: str) -> tuple[str, str]:
    """Return (bucket_name, blob_path) from Airtable gcp_raw_location."""
    location = (location or "").strip()
    if not location:
        raise ValueError("gcp_raw_location is empty")

    if location.startswith("gs://"):
        location = location[len("gs://") :]

    if "/" not in location:
        raise ValueError(f"unexpected gcp_raw_location format: {location}")

    bucket_name, blob_path = location.split("/", 1)
    if not bucket_name or not blob_path:
        raise ValueError(f"unexpected gcp_raw_location format: {location}")
    return bucket_name, blob_path


def fetch_meta_fail_missing_storage(limit: int | None = None):
    formula = (
        "AND("
        f"{{status}} = '{VideoStatus.META_FAIL}',"
        "NOT({gcp_storage_video_location}),"
        "{gcp_raw_location}"
        ")"
    )
    print(f"Using airtable formula: {formula}")
    if limit:
        print(f"Limiting Airtable results to first {limit} rows")

    try:
        records = airtable_services.video_table.all(formula=formula, max_records=limit)
    except TypeError:
        records = airtable_services.video_table.all(formula=formula)
        if limit is not None:
            records = records[:limit]

    rows = []
    for record in records:
        fields = dict(record.get("fields") or {})
        record_id = record.get("id")
        fields["_airtable_record_id"] = record_id
        if "unique_video_id" not in fields and record_id:
            fields["unique_video_id"] = record_id

        subject_id_list = fields.get("subject_id", [])
        participant_id = subject_id_list[0] if subject_id_list else "Unknown"
        fields["subject_id"] = airtable_services.participant_dict.get(participant_id, fields.get("subject_id"))

        rows.append(fields)

    return rows


def _truncate_airtable_text(value: str, *, max_len: int = STATUS_TEST_MAX_LEN) -> str:
    value = (value or "").strip()
    if len(value) <= max_len:
        return value
    return value[: max_len - 3] + "..."


def _update_airtable(record_id: str, fields: dict, *, dry_run: bool) -> bool:
    fields = dict(fields)
    for key in ("status_test", "gcp_storage_video_location"):
        if key in fields and fields[key] is not None:
            fields[key] = _truncate_airtable_text(str(fields[key]))

    if dry_run:
        print(f"[DRY_RUN] would update {record_id}: {fields}")
        return True

    try:
        airtable_services.update_video_table_single_video(record_id, fields)
        return True
    except Exception as e:
        logger.error("airtable_update_failed record_id=%s error=%s fields=%s", record_id, e, fields)
        return False


def process_one(row: dict, *, dry_run: bool) -> tuple[bool, str, str | None]:
    """
    Returns (success, message, failed_step).
    failed_step is set on failure (e.g. 'compress', 'rotate', 'download').
    """
    record_id = row.get("_airtable_record_id") or row.get("unique_video_id")
    if not record_id:
        return False, "missing Airtable record id", None

    raw_loc = row.get("gcp_raw_location")
    if not raw_loc or (isinstance(raw_loc, float) and str(raw_loc) == "nan"):
        return False, "gcp_raw_location is empty", None

    try:
        raw_bucket, raw_blob = _parse_gcs_location(str(raw_loc))
    except ValueError as e:
        return False, str(e), None

    video = Video(video_info=row)
    if not video.subject_id:
        return False, "subject_id is missing", None

    work_dir = os.path.join(WORK_ROOT, record_id)
    raw_dir = os.path.join(work_dir, "raw")
    processed_dir = os.path.join(work_dir, "processed")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)

    local_raw = os.path.join(raw_dir, os.path.basename(raw_blob))
    video.local_raw_download_path = local_raw
    video.local_processed_folder = processed_dir

    storage_bucket = f"{video.gcp_bucket_name}_storage"

    if dry_run:
        return True, (
            f"would download gs://{raw_bucket}/{raw_blob} -> compress/rotate/blackout -> "
            f"upload {storage_bucket}/{video.subject_id}/<processed>.mp4"
        ), None

    try:
        ok, msg = storage.download_file_from_gcs(raw_bucket, raw_blob, local_raw)
        if not ok:
            return False, f"download failed: {msg}", "download"

        processor = FileProcessor(video)
        ok, step, process_err = compress_rotate_blackout_video(video, processor)
        if not ok:
            return False, process_err or f"{step} failed", step

        dest_blob = f"{video.subject_id}/{os.path.basename(video.compress_video_path)}"
        full_storage_location = f"{storage_bucket}/{dest_blob}"

        ok, upload_err = storage.upload_file_to_gcs(
            video.compress_video_path, dest_blob, storage_bucket
        )
        if not ok:
            return False, f"upload failed: {upload_err}", "upload"

        video.gcp_storage_video_location = dest_blob
        video_size_mb, _, size_err = storage.get_object_sizes(storage_bucket, dest_blob, None)
        if size_err:
            logger.warning("size check failed for %s: %s", record_id, size_err)

        update_fields = {
            "gcp_storage_video_location": full_storage_location,
            "pipeline_run_date": _today_pipeline_run_date(),
            "status_test": STATUS_TEST_SUCCESS,
            "video_size_mb": video_size_mb,
        }
        if not _update_airtable(record_id, update_fields, dry_run=False):
            logger.error(
                "compressed upload ok but airtable update failed record_id=%s location=%s",
                record_id,
                full_storage_location,
            )
        return True, full_storage_location, None

    finally:
        if os.path.isdir(work_dir):
            shutil.rmtree(work_dir, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Compress raw GCS videos for meta-fail rows missing gcp_storage_video_location"
        )
    )
    parser.add_argument("--limit", type=int, default=None, help="Max Airtable rows to process")
    parser.add_argument("--dry_run", action="store_true", help="Query only; do not download/upload/update")
    args = parser.parse_args()

    rows = fetch_meta_fail_missing_storage(limit=args.limit)
    print(f"Loaded {len(rows)} eligible videos from Airtable.")
    if not rows:
        return

    ok_count = 0
    fail_count = 0

    iterator = tqdm(rows, desc="compress_meta_fail_batch", unit="video")
    for row in iterator:
        record_id = row.get("_airtable_record_id") or row.get("unique_video_id")
        label = row.get("gopro_video_id") or record_id
        iterator.set_postfix_str(str(label))

        success, message, failed_step = process_one(row, dry_run=args.dry_run)
        if success:
            ok_count += 1
            if args.dry_run:
                print(f"[DRY_RUN OK] {record_id}: {message}")
            else:
                logger.info("compressed_batch_ok record_id=%s location=%s", record_id, message)
        else:
            fail_count += 1
            logger.error(
                "compressed_batch_fail record_id=%s step=%s error=%s",
                record_id,
                failed_step,
                message,
            )
            if not args.dry_run:
                if failed_step == "compress":
                    update_fields = {
                        "status": VideoStatus.COMPRESS_FAIL,
                        "gcp_storage_video_location": message,
                        "pipeline_run_date": _today_pipeline_run_date(),
                    }
                else:
                    update_fields = {
                        "status_test": f"compressed_batch_fail: {failed_step}: {message}",
                    }
                if not _update_airtable(record_id, update_fields, dry_run=False):
                    logger.error(
                        "could not record failure in Airtable for %s; see log above",
                        record_id,
                    )
            else:
                print(f"[DRY_RUN FAIL] {record_id}: {message}")

    print(f"Done. success={ok_count} failed={fail_count} dry_run={args.dry_run}")


if __name__ == "__main__":
    main()
