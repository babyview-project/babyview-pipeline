#!/usr/bin/env python3
"""
metadata_extraction_from_gcp_storage.py

Adds a sanity check after producing the metadata zip:
- If the local zip size is under --min_zip_kb (default 3 KB), it retries the metadata extraction
  up to --max_small_zip_retries times (default 1).

Also includes Airtable filter:
- imu_comment is not blank
- imu_issue_fix_date is blank

Behavior:
- download RAW from gcp_raw_location
- FileProcessor(video).extract_meta()
- zip metadata folder
- overwrite upload to gcp_storage_zip_location
- update Airtable: metadata_size_kb, imu_issue_fix_date (YYYY-MM-DD, Pacific)

You can rename this file to metadata_extraction_from_gcp_storage.py if you want.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from tqdm import tqdm
from google.api_core.exceptions import Forbidden, NotFound

from airtable_services import AirtableServices
from gcp_storage_services import GCPStorageServices

from controllers import FileProcessor
from video import Video

airtable = AirtableServices()

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def parse_gcs_location(value: str) -> Optional[Tuple[str, str]]:
    if not value:
        return None
    v = str(value).strip().strip('"').strip("'")
    if v.startswith("gs://"):
        v = v[len("gs://"):]
    if "/" not in v:
        return None
    bucket, blob = v.split("/", 1)
    bucket = bucket.strip()
    blob = blob.strip().lstrip("/")
    if not bucket or not blob:
        return None
    return bucket, blob


def kb(size_bytes: int) -> float:
    return round(size_bytes / 1024, 3)


def now_date_pacific() -> str:
    tz = pytz.timezone("America/Los_Angeles")
    return datetime.now(tz).date().isoformat()  # YYYY-MM-DD


def coerce_date_fallback(fields: Dict[str, Any]) -> Optional[str]:
    candidates = [
        "date",
        "recording_date",
        "recordingDate",
        "source_date",
        "sourceDate",
        "start_date",
        "startDate",
        "logging_date",
        "pipeline_run_date",
    ]
    for k in candidates:
        v = fields.get(k)
        if v:
            return v
    return None


def build_video_info_from_airtable_record(rec: Dict[str, Any], airtable: AirtableServices) -> Dict[str, Any]:
    fields: Dict[str, Any] = rec.get("fields", {}) or {}
    video_info = dict(fields)

    video_info["unique_video_id"] = rec.get("id") or fields.get("unique_video_id")

    subj_val = fields.get("subject_id")
    if isinstance(subj_val, list):
        participant_id = subj_val[0] if subj_val else None
        mapped = airtable.participant_dict.get(participant_id) if participant_id else None
        video_info["subject_id"] = mapped or participant_id or ""
    elif subj_val:
        video_info["subject_id"] = subj_val
    else:
        video_info["subject_id"] = ""

    if not video_info.get("gopro_video_id"):
        for k in ("goproVideoId", "gopro_id", "goproId", "video_id", "videoId"):
            if fields.get(k):
                video_info["gopro_video_id"] = fields.get(k)
                break

    if not video_info.get("dataset"):
        for k in ("Dataset", "data_set"):
            if fields.get(k):
                video_info["dataset"] = fields.get(k)
                break

    if not video_info.get("date"):
        video_info["date"] = coerce_date_fallback(fields)

    return video_info


def download_blob(storage: GCPStorageServices, bucket: str, blob_name: str, dst_path: str) -> None:
    ensure_dir(os.path.dirname(dst_path))
    bkt = storage.client.bucket(bucket)
    blob = bkt.blob(blob_name)
    blob.download_to_filename(dst_path, timeout=1800)


def extract_metadata_to_zip_via_fileprocessor(video: Video, out_dir: str) -> Tuple[str, str]:
    uniq_id = video.unique_video_id or "unknown_unique_id"

    subject = (video.subject_id or "unknown_subject").strip() or "unknown_subject"
    gopro = str(video.gopro_video_id or "unknown_gopro").strip() or "unknown_gopro"
    local_processed_folder = os.path.join(out_dir, "processed", subject, gopro)
    ensure_dir(local_processed_folder)
    video.local_processed_folder = local_processed_folder

    meta_folder = os.path.join(local_processed_folder, f"{video.gcp_file_name}_metadata")
    ensure_dir(meta_folder)

    fp = FileProcessor(video=video)
    _, error_msg = fp.extract_meta()
    if error_msg:
        # Mark META_FAIL and stop; do NOT upload metadata zip
        payload = {
            "status": "error_in_meta_extraction",  # same as VideoStatus.META_FAIL
            # optionally store details if you have a notes/error field:
            "gcp_storage_zip_location": error_msg,
            "imu_issue_fix_date": "2000-01-01"
        }
        airtable.update_video_table_single_video(uniq_id, payload)
        raise RuntimeError(error_msg)

    if not os.path.isdir(meta_folder):
        raise RuntimeError(f"Expected metadata folder not found: {meta_folder}")

    zip_dir = os.path.join(out_dir, "metadata_zips")
    ensure_dir(zip_dir)
    base_name = os.path.join(zip_dir, f"{uniq_id}_metadata")
    zip_path = shutil.make_archive(base_name=base_name, format="zip", root_dir=meta_folder)
    return zip_path, meta_folder


def local_file_kb(path: str) -> float:
    try:
        return round(os.path.getsize(path) / 1024, 3)
    except Exception:
        return 0.0


def extract_zip_with_small_size_retry(video: Video, out_dir: str, min_zip_kb: float, max_retries: int) -> Tuple[str, str, float]:
    zip_path, meta_folder = extract_metadata_to_zip_via_fileprocessor(video=video, out_dir=out_dir)
    size_kb = local_file_kb(zip_path)

    retries = 0
    while size_kb < min_zip_kb and retries < max_retries:
        retries += 1

        # Remove zip + clear metadata folder to avoid re-zipping an empty folder
        try:
            os.remove(zip_path)
        except Exception:
            pass
        try:
            shutil.rmtree(meta_folder)
        except Exception:
            pass
        ensure_dir(meta_folder)

        zip_path, meta_folder = extract_metadata_to_zip_via_fileprocessor(video=video, out_dir=out_dir)
        size_kb = local_file_kb(zip_path)

    return zip_path, meta_folder, size_kb


def upload_zip_and_get_size_kb(storage: GCPStorageServices, zip_path: str, dest_bucket: str, dest_blob: str) -> float:
    ok, msg = storage.upload_file_to_gcs(zip_path, dest_blob, dest_bucket)
    if not ok:
        raise RuntimeError(f"upload failed: {msg}")

    bkt = storage.client.bucket(dest_bucket)
    blob = bkt.get_blob(dest_blob)
    if blob is None:
        raise RuntimeError(f"uploaded blob not found after upload: gs://{dest_bucket}/{dest_blob}")

    return kb(int(blob.size or 0))


def build_formula_imu_true_and_unfixed(imu_issue_field_name: str, imu_issue_fix_date_field_name: str) -> str:
    return f"AND(NOT({{{imu_issue_field_name}}}=BLANK()), {{{imu_issue_fix_date_field_name}}}=BLANK())"


@dataclass
class Result:
    uniq_id: str
    ok: bool
    raw_bucket: Optional[str] = None
    raw_blob: Optional[str] = None
    zip_bucket: Optional[str] = None
    zip_blob: Optional[str] = None
    local_video: Optional[str] = None
    local_zip: Optional[str] = None
    local_zip_kb: Optional[float] = None
    uploaded_zip_kb: Optional[float] = None
    err: Optional[str] = None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out_dir", default="data/imu_fix", help="Local output dir for downloads/zips.")
    ap.add_argument("--max_records", type=int, default=0, help="0 means no limit; otherwise cap processing.")
    ap.add_argument("--dry_run", action="store_true", help="No download/extract/upload/update; print actions only.")
    ap.add_argument("--no_keep_video", action="store_true", help="Delete downloaded raw videos after zipping.")
    ap.add_argument("--uniq_id", default=None, help="Airtable record id (e.g. recxxxxxxxxxxxxxxxx). If provided, only process this one video.")

    ap.add_argument("--min_zip_kb", type=float, default=3.0, help="If local metadata zip is smaller than this KB, retry extraction.")
    ap.add_argument("--max_small_zip_retries", type=int, default=1, help="Max retries when zip size is below --min_zip_kb.")

    ap.add_argument("--raw_location_field_name", default="gcp_raw_location")
    ap.add_argument("--compress_vid_location_field_name", default="gcp_storage_video_location")
    ap.add_argument("--zip_location_field_name", default="gcp_storage_zip_location")
    ap.add_argument("--imu_issue_field_name", default="imu_comment")
    ap.add_argument("--metadata_size_field_name", default="metadata_size_kb")
    ap.add_argument("--imu_issue_fix_date_field_name", default="imu_issue_fix_date")
    args = ap.parse_args()

    ensure_dir(args.out_dir)

    storage = GCPStorageServices()

    # If a specific uniq_id is provided, process only that record
    if args.uniq_id:
        formula = f"record_id={args.uniq_id}"
        try:
            rec = airtable.video_table.get(args.uniq_id)
        except Exception as e:
            raise RuntimeError(f"Failed to fetch Airtable record {args.uniq_id!r}: {e}")
        records = [rec]
        print(f"Loaded 1 Airtable record: {args.uniq_id}")

    else:
        formula = build_formula_imu_true_and_unfixed(args.imu_issue_field_name, args.imu_issue_fix_date_field_name)
        print(f"Using Airtable formula:\n  {formula}\n")

        records = airtable.video_table.all(formula=formula)
        if args.max_records and args.max_records > 0:
            records = records[: args.max_records]
        print(f"Loaded {len(records)} Airtable records")

    results: List[Result] = []
    updated = 0
    failed = 0

    for rec in tqdm(records, desc="Fix IMU metadata"):
        uniq_id = rec.get("id") or "unknown_record_id"
        fields: Dict[str, Any] = rec.get("fields", {}) or {}

        try:
            raw_loc = fields.get(args.raw_location_field_name)
            parsed_raw = parse_gcs_location(raw_loc) if isinstance(raw_loc, str) else None
            if not parsed_raw:
                raise RuntimeError(f"Missing/unparsable raw location {args.raw_location_field_name}: {raw_loc}")
            raw_bucket, raw_blob = parsed_raw

            zip_loc = fields.get(args.zip_location_field_name)
            compressed_vid_loc = fields.get(args.compress_vid_location_field_name)
            parsed_zip = parse_gcs_location(zip_loc) if isinstance(zip_loc, str) else None
            parsed_compressed_vid = parse_gcs_location(compressed_vid_loc) if isinstance(compressed_vid_loc, str) else None
            if parsed_zip:
                zip_bucket, zip_blob = parsed_zip
            elif parsed_compressed_vid:
                zip_bucket, comp_blob = parsed_compressed_vid
                zip_blob = comp_blob.split('.')[0] + '_metadata.zip'
            else:
                raise RuntimeError(f"Missing/unparsable zip location {args.zip_location_field_name}: {zip_loc}")

            local_video_path = os.path.join(args.out_dir, "raw_videos", raw_bucket, raw_blob)

            if args.dry_run:
                print(f"DRY_RUN {uniq_id}: download RAW gs://{raw_bucket}/{raw_blob} -> {local_video_path}")
                print(f"DRY_RUN {uniq_id}: extract metadata (retry if zip < {args.min_zip_kb} KB), upload -> gs://{zip_bucket}/{zip_blob}")
                results.append(Result(
                    uniq_id=uniq_id, ok=True,
                    raw_bucket=raw_bucket, raw_blob=raw_blob,
                    zip_bucket=zip_bucket, zip_blob=zip_blob,
                    local_video=local_video_path,
                ))
                continue

            video_info = build_video_info_from_airtable_record(rec, airtable=airtable)
            video = Video(video_info=video_info)

            download_blob(storage, raw_bucket, raw_blob, local_video_path)
            video.local_raw_download_path = local_video_path

            local_zip_path, _, local_zip_kb = extract_zip_with_small_size_retry(
                video=video,
                out_dir=args.out_dir,
                min_zip_kb=args.min_zip_kb,
                max_retries=max(0, args.max_small_zip_retries),
            )

            if local_zip_kb < args.min_zip_kb:
                raise RuntimeError(f"Metadata zip too small after retries: {local_zip_kb} KB (< {args.min_zip_kb} KB).")

            uploaded_kb = upload_zip_and_get_size_kb(storage, local_zip_path, zip_bucket, zip_blob)

            payload = {
                args.metadata_size_field_name: uploaded_kb,
                args.imu_issue_fix_date_field_name: now_date_pacific(),
            }
            airtable.update_video_table_single_video(uniq_id, payload)
            updated += 1

            if args.no_keep_video:
                try:
                    os.remove(local_video_path)
                except Exception:
                    pass

            results.append(Result(
                uniq_id=uniq_id, ok=True,
                raw_bucket=raw_bucket, raw_blob=raw_blob,
                zip_bucket=zip_bucket, zip_blob=zip_blob,
                local_video=None if args.no_keep_video else local_video_path,
                local_zip=local_zip_path,
                local_zip_kb=local_zip_kb,
                uploaded_zip_kb=uploaded_kb,
            ))

        except (NotFound, Forbidden) as e:
            failed += 1
            results.append(Result(uniq_id=uniq_id, ok=False, err=f"GCS error: {type(e).__name__}: {e}"))
        except Exception as e:
            failed += 1
            results.append(Result(uniq_id=uniq_id, ok=False, err=str(e)))

    summary_path = os.path.join(args.out_dir, "run_summary_100.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "formula": formula,
                "count_total": len(results),
                "count_updated_airtable": updated,
                "count_failed": failed,
                "results": [r.__dict__ for r in results],
            },
            f,
            indent=2,
        )

    print("\nDone.")
    print(f"Updated Airtable: {updated}")
    print(f"Failed: {failed}")
    print(f"Summary JSON: {summary_path}")


if __name__ == "__main__":
    main()
