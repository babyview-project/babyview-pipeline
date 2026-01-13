# databrary_backfill.py

import os
from typing import List

from datetime import datetime
import pytz

from gcp_storage_services import GCPStorageServices
from airtable_services import airtable_services
from video import Video
from databrary_client import DatabraryClient
from status_types import VideoStatus

storage = GCPStorageServices()
dc = DatabraryClient()


def _download_from_gcs_to_temp(gcp_storage_video_location: str) -> tuple[str | None, str | None]:
    """
    gcp_storage_video_location stored in Airtable looks like:
        "<bucket_name>_storage/subject_id/filename.mp4"

    Returns:
        (local_path or None, error_message or None)
    """
    if not gcp_storage_video_location:
        return None, "DOWNLOAD: gcp_storage_video_location is empty"

    parts = gcp_storage_video_location.split("/", 1)
    if len(parts) != 2:
        return None, f"DOWNLOAD: unexpected gcp_storage_video_location format: {gcp_storage_video_location}"

    bucket_name, blob_path = parts[0], parts[1]

    tmp_dir = "tmp_databrary"
    os.makedirs(tmp_dir, exist_ok=True)
    local_path = os.path.join(tmp_dir, os.path.basename(blob_path))

    success, msg = storage.download_file_from_gcs(bucket_name, blob_path, local_path)
    if not success:
        return None, f"DOWNLOAD: failed from GCS {bucket_name}/{blob_path}: {msg}"

    return local_path, None


def _mark_airtable_error(video_record_id: str, error_msg: str):
    """
    Directly update Airtable when we can't even get to DatabraryClient
    (e.g., download from GCS failed).
    """
    try:
        tz = pytz.timezone("America/Los_Angeles")
        now_str = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

        airtable_services.update_video_table_single_video(
            video_record_id,
            {
                "databrary_upload_date": now_str,
                "databrary_upload_status_url": f"ERROR: {error_msg}",
            },
        )
    except Exception as e:
        print(f"[AIRTABLE_UPDATE_ERROR] {video_record_id}: {e}")


def backfill_databrary_for_video_ids(video_record_ids: List[str], patch_only: bool = False):
    """
    Backfill Databrary uploads for specific Airtable video record IDs.

    Steps per record:
      - read Airtable row
      - download compressed mp4 from GCS to temp
      - build Video object with that local path
      - call DatabraryClient.upload_video(video)
      - remove local file
    """
    for vid_id in video_record_ids:
        print(f"=== Backfill Databrary for {vid_id} ===")
        try:
            record = airtable_services.video_table.get(vid_id)
        except Exception as e:
            print(f"[ERROR] Failed to fetch Airtable record {vid_id}: {e}")
            continue

        fields = record.get("fields", {})
        gcp_storage_video_location = fields.get("gcp_storage_video_location")
        databrary_upload_date = fields.get("databrary_upload_date")
        if not patch_only and databrary_upload_date:
            print(f"[SKIP] {vid_id}: it has been uploaded on {databrary_upload_date}")
            continue

        if not gcp_storage_video_location:
            msg = "DOWNLOAD: no gcp_storage_video_location in Airtable"
            print(f"[SKIP] {vid_id}: {msg}")
            _mark_airtable_error(vid_id, msg)
            continue

        # 1) Decide path for DatabraryClient
        #    - default: download compressed mp4 from GCS to temp
        #    - patch_only: skip download; use gcp_storage_video_location for filename matching
        if patch_only:
            local_path = gcp_storage_video_location
        else:
            local_path, dl_err = _download_from_gcs_to_temp(gcp_storage_video_location)
            if dl_err:
                print(f"[ERROR] {vid_id}: {dl_err}")
                _mark_airtable_error(vid_id, dl_err)
                continue

        # 2) Build Video object from Airtable fields
        try:
            video_info = fields.copy()
            # ensure the Video object uses the Airtable record ID as unique_video_id
            video_info["unique_video_id"] = vid_id
            subject_id_list = video_info.get("subject_id", [])

            participant_id = subject_id_list[0] if subject_id_list else "Unknown"
            video_info["subject_id"] = airtable_services.participant_dict.get(participant_id, None)

            video = Video(video_info=video_info)
            # inject the local compressed path for DatabraryClient
            video.compress_video_path = local_path
        except Exception as e:
            msg = f"VIDEO_BUILD: failed to construct Video object: {e}"
            print(f"[ERROR] {vid_id}: {msg}")
            _mark_airtable_error(vid_id, msg)
            # clean up temp file (download mode only)
            if not patch_only:
                try:
                    os.remove(local_path)
                except Exception:
                    pass
            continue
        print(f"Video Info being uploaded: {video.to_dict()}")

        # 3) Call DatabraryClient (this will ALWAYS write databrary_* fields)
        status_url, error_log = dc.upload_video(video, patch_only=patch_only)
        if error_log:
            print(f"[Databrary] {vid_id}: errors -> {' | '.join(error_log)}")
        else:
            print(f"[Databrary] {vid_id}: success -> {status_url}")

        # 4) Clean up local file (download mode only)
        if not patch_only:
            try:
                os.remove(local_path)
            except Exception:
                pass


def backfill_databrary_for_release(release_name: str, limit: int | None = None, patch_only: bool = False):
    """
    Use the Release table (tblVeWx2MbrXRa6o1) to find the row with Name=release_name,
    get its linked 'Videos' field (list of video record IDs), and run Databrary backfill
    on those videos.
    """
    video_ids = airtable_services.get_video_ids_for_a_release_set(release_name)
    if not video_ids:
        print(f"[INFO] No videos found for release '{release_name}'")
        return

    if limit is not None:
        video_ids = video_ids[:limit]

    input(f"[INFO] Release '{release_name}' has {len(video_ids)} videos: {video_ids}")
    backfill_databrary_for_video_ids(video_ids, patch_only=patch_only)


def backfill_databrary_auto(status_test=None, limit: int | None = None, patch_only: bool = False):
    """
    Auto-select processed videos that:
      - status == successfully_processed
      - have a gcp_storage_video_location
      - have no databrary_upload_status_url yet

    and run the same backfill.

    'limit' lets you test on just a few.
    """
    status_value = VideoStatus.PROCESSED  # "successfully_processed"
    if not status_test:
        formula = (
            "AND("
            f"{{status}} = '{status_value}',"
            "{gcp_storage_video_location},"
            "NOT({databrary_upload_date})"
            ")"
        )
    else:
        formula = (
            "AND("
            f"{{status}} = '{status_value}',"
            f"{{status_test}} = {status_test},"
            "{gcp_storage_video_location},"
            "{databrary_upload_date}"
            # "NOT({databrary_upload_date})"
            ")"
        )

    print(f"[INFO] Airtable formula: {formula}")
    try:
        records = airtable_services.video_table.all(formula=formula)
    except Exception as e:
        print(f"[ERROR] Failed to fetch records for auto backfill: {e}")
        return

    if not records:
        print("[INFO] No candidate videos found for Databrary backfill.")
        return

    if limit is not None:
        records = records[:limit]

    video_ids = [r["id"] for r in records]
    print(f"[INFO] Found {len(video_ids)} candidate videos: {video_ids}")

    backfill_databrary_for_video_ids(video_ids, patch_only=patch_only)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Databrary backfill from GCS for existing videos")
    parser.add_argument(
        "--unique_video_id",
        action="append",
        help="Airtable video record ID to backfill (can repeat)",
    )
    parser.add_argument(
        "--release",
        type=str,
        help='Release Name in release table (e.g. "2025.1") to backfill all linked videos',
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Auto-select videos (status processed & no databrary status) and backfill",
    )
    parser.add_argument(
        "--status_test",
        type=int,
        help="Manual selected videos (status processed & no databrary status) and backfill",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit when using --auto",
    )
    parser.add_argument(
        "--patch_only",
        action="store_true",
        help="Patch-only mode: skip initiate/upload/PUT; locate existing Databrary file and PATCH source_date.",
    )

    args = parser.parse_args()

    if args.unique_video_id:
        backfill_databrary_for_video_ids(args.unique_video_id, patch_only=args.patch_only)
    elif args.release:
        backfill_databrary_for_release(args.release, limit=args.limit, patch_only=args.patch_only)
    elif args.status_test:
        backfill_databrary_auto(status_test=args.status_test, limit=args.limit, patch_only=args.patch_only)
    elif args.auto:
        backfill_databrary_auto(limit=args.limit, patch_only=args.patch_only)
    else:
        print("Provide either --video-id (one or more) or --auto")
