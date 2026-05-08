#!/usr/bin/env python3
"""
Patch metadata zips in GCS by adding imu_combined.csv.

Flow:
1) Query Airtable video records where gcp_storage_zip_location contains "_metadata.zip"
   but not "_metadata_imu.zip".
2) Download zip from GCS.
3) Unzip and run IMU processing from metadata txt files.
4) Write imu_combined.csv into extracted metadata folder.
5) Re-zip as *_metadata_imu.zip and upload to same bucket/path directory.
6) Delete original *_metadata.zip blob from GCS.
7) Update Airtable:
   - gcp_storage_zip_location -> new *_metadata_imu.zip path
   - comment -> "no_grav" or error string (or None when no issue)
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from tqdm import tqdm

from airtable_services import AirtableServices
from gcp_storage_services import GCPStorageServices
from imu.utils import process_imu_for_video_dir


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


def make_imu_blob_name(old_blob: str) -> str:
    if old_blob.endswith("_metadata.zip"):
        return old_blob[:-len("_metadata.zip")] + "_metadata_imu.zip"
    if old_blob.endswith(".zip"):
        return old_blob[:-len(".zip")] + "_imu.zip"
    return old_blob + "_imu.zip"


def find_metadata_dir(unzip_root: str) -> Optional[str]:
    # Prefer folder containing ACCL_meta.txt.
    for root, _, files in os.walk(unzip_root):
        if "ACCL_meta.txt" in files:
            return root
    return None


def rezip_dir(src_dir: str, out_zip_path: str) -> None:
    with zipfile.ZipFile(out_zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for root, _, files in os.walk(src_dir):
            for fn in files:
                abs_path = os.path.join(root, fn)
                rel_path = os.path.relpath(abs_path, src_dir)
                zf.write(abs_path, arcname=rel_path)


@dataclass
class PatchResult:
    record_id: str
    ok: bool
    old_zip: Optional[str] = None
    new_zip: Optional[str] = None
    comment: Optional[str] = None
    err: Optional[str] = None


def build_formula(zip_field_name: str) -> str:
    return (
        "AND("
        f"NOT({{{zip_field_name}}}=BLANK()),"
        f"FIND('_metadata.zip', {{{zip_field_name}}}),"
        f"NOT(FIND('_metadata_imu.zip', {{{zip_field_name}}}))"
        ")"
    )


def today_iso_date() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def mark_failed_in_airtable(
    airtable: AirtableServices,
    record_id: str,
    comment_field_name: str,
    err: str,
) -> None:
    airtable.update_video_table_single_video(
        record_id,
        {
            comment_field_name: err,
            "metadata_issue_fix_date": "2000/1/1",
        },
    )


def patch_one_record(
    storage: GCPStorageServices,
    airtable: AirtableServices,
    rec: Dict[str, Any],
    zip_field_name: str,
    comment_field_name: str,
    dry_run: bool,
) -> PatchResult:
    record_id = rec.get("id") or "unknown_record"
    fields: Dict[str, Any] = rec.get("fields", {}) or {}
    zip_loc = fields.get(zip_field_name)

    if not isinstance(zip_loc, str):
        err = f"Missing/unparsable {zip_field_name}: {zip_loc}"
        mark_failed_in_airtable(airtable, record_id, comment_field_name, err)
        return PatchResult(record_id=record_id, ok=False, err=err)

    parsed = parse_gcs_location(zip_loc)
    if not parsed:
        err = f"Missing/unparsable {zip_field_name}: {zip_loc}"
        mark_failed_in_airtable(airtable, record_id, comment_field_name, err)
        return PatchResult(record_id=record_id, ok=False, err=err)

    bucket, old_blob = parsed
    new_blob = make_imu_blob_name(old_blob)
    new_zip_loc = f"{bucket}/{new_blob}"

    if dry_run:
        return PatchResult(
            record_id=record_id,
            ok=True,
            old_zip=f"{bucket}/{old_blob}",
            new_zip=new_zip_loc,
            comment="DRY_RUN",
        )

    with tempfile.TemporaryDirectory(prefix="imu_zip_patch_") as tmpdir:
        local_old_zip = os.path.join(tmpdir, "old.zip")
        unzip_dir = os.path.join(tmpdir, "unzipped")
        local_new_zip = os.path.join(tmpdir, "new_imu.zip")

        os.makedirs(unzip_dir, exist_ok=True)

        ok, msg = storage.download_file_from_gcs(bucket, old_blob, local_old_zip)
        if not ok:
            err = f"download_failed: {msg}"
            mark_failed_in_airtable(airtable, record_id, comment_field_name, err)
            return PatchResult(record_id=record_id, ok=False, old_zip=f"{bucket}/{old_blob}", err=err)

        try:
            with zipfile.ZipFile(local_old_zip, "r") as zf:
                zf.extractall(unzip_dir)
        except Exception as e:
            err = f"unzip_failed: {e}"
            mark_failed_in_airtable(airtable, record_id, comment_field_name, err)
            return PatchResult(record_id=record_id, ok=False, old_zip=f"{bucket}/{old_blob}", err=err)

        metadata_dir = find_metadata_dir(unzip_dir)
        if not metadata_dir:
            err = "imu_failed: ACCL_meta.txt not found in zip"
            mark_failed_in_airtable(airtable, record_id, comment_field_name, err)
            return PatchResult(record_id=record_id, ok=False, old_zip=f"{bucket}/{old_blob}", err=err)

        try:
            imu_df = process_imu_for_video_dir(metadata_dir)
            if imu_df is None:
                raise RuntimeError("IMU processing returned None")
            imu_comment = imu_df.attrs.get("comment")
            imu_csv_path = os.path.join(metadata_dir, "imu_combined.csv")
            imu_df.to_csv(imu_csv_path, index=False)
        except Exception as e:
            err = f"imu_failed: {e}"
            mark_failed_in_airtable(airtable, record_id, comment_field_name, err)
            return PatchResult(record_id=record_id, ok=False, old_zip=f"{bucket}/{old_blob}", err=err)

        try:
            rezip_dir(unzip_dir, local_new_zip)
        except Exception as e:
            err = f"rezip_failed: {e}"
            mark_failed_in_airtable(airtable, record_id, comment_field_name, err)
            return PatchResult(record_id=record_id, ok=False, old_zip=f"{bucket}/{old_blob}", err=err)

        ok, msg = storage.upload_file_to_gcs(local_new_zip, new_blob, bucket)
        if not ok:
            err = f"upload_failed: {msg}"
            mark_failed_in_airtable(airtable, record_id, comment_field_name, err)
            return PatchResult(record_id=record_id, ok=False, old_zip=f"{bucket}/{old_blob}", new_zip=new_zip_loc, err=err)

        try:
            storage.client.bucket(bucket).blob(old_blob).delete()
        except Exception as e:
            err = f"delete_old_zip_failed: {e}"
            airtable.update_video_table_single_video(
                record_id,
                {
                    zip_field_name: new_zip_loc,
                    comment_field_name: err,
                    "metadata_issue_fix_date": "2000/1/1",
                },
            )
            return PatchResult(
                record_id=record_id,
                ok=False,
                old_zip=f"{bucket}/{old_blob}",
                new_zip=new_zip_loc,
                comment=err,
                err=err,
            )

        airtable.update_video_table_single_video(
            record_id,
            {
                zip_field_name: new_zip_loc,
                comment_field_name: imu_comment,
                "metadata_issue_fix_date": today_iso_date(),
            },
        )
        return PatchResult(
            record_id=record_id,
            ok=True,
            old_zip=f"{bucket}/{old_blob}",
            new_zip=new_zip_loc,
            comment=imu_comment,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Patch metadata zip with imu_combined.csv.")
    parser.add_argument("--max_records", type=int, default=0, help="0 means no limit.")
    parser.add_argument("--dry_run", action="store_true", help="Do not modify GCS or Airtable.")
    parser.add_argument("--uniq_id", type=str, default=None, help="Optional Airtable record id to process.")
    parser.add_argument("--zip_field_name", type=str, default="gcp_storage_zip_location")
    parser.add_argument("--comment_field_name", type=str, default="comment")
    args = parser.parse_args()

    airtable = AirtableServices()
    storage = GCPStorageServices()

    if args.uniq_id:
        rec = airtable.video_table.get(args.uniq_id)
        records = [rec]
        formula = f"record_id={args.uniq_id}"
    else:
        formula = build_formula(args.zip_field_name)
        records = airtable.video_table.all(formula=formula)
        if args.max_records > 0:
            records = records[:args.max_records]

    print(f"Using Airtable formula: {formula}")
    print(f"Loaded {len(records)} records")

    results: List[PatchResult] = []
    for rec in tqdm(records, desc="Patching metadata zips"):
        res = patch_one_record(
            storage=storage,
            airtable=airtable,
            rec=rec,
            zip_field_name=args.zip_field_name,
            comment_field_name=args.comment_field_name,
            dry_run=args.dry_run,
        )
        results.append(res)

    ok_count = sum(1 for r in results if r.ok)
    fail_count = len(results) - ok_count
    print(f"Done. ok={ok_count}, failed={fail_count}")
    print(
        json.dumps(
            {
                "ok": ok_count,
                "failed": fail_count,
                "results": [r.__dict__ for r in results],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
