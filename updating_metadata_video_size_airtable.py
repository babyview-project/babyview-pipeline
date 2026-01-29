#!/usr/bin/env python3
"""
updating_metadata_video_size_airtable.py

Purpose
- Query Airtable "video" table for records where:
    {status} = 'successfully_processed'
    AND imu_issue is false/blank
- For each record:
    - Read GCS locations for storage video + metadata zip (Airtable fields)
    - Fetch object sizes from GCS (no name scanning)
    - Update Airtable fields:
        - video_size_mb
        - metadata_size_kb
  (Does NOT modify imu_issue)

Defaults assume these Airtable fields:
  - status
  - imu_issue (checkbox)
  - gcp_storage_video_location  (gs://bucket/blob OR bucket/blob)
  - gcp_storage_zip_location    (gs://bucket/blob OR bucket/blob)
  - video_size_mb
  - metadata_size_kb

Usage
  python updating_metadata_video_size_airtable.py

Optional:
  --dry_run
  --max_records 200
  --video_location_field_name gcp_storage_video_location
  --zip_location_field_name gcp_storage_zip_location
  --video_size_field_name video_size_mb
  --metadata_size_field_name metadata_size_kb
  --status_field_name status
  --status_value successfully_processed
  --imu_issue_field_name imu_issue
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from tqdm import tqdm
from google.api_core.exceptions import Forbidden, NotFound

from airtable_services import AirtableServices
from gcp_storage_services import GCPStorageServices


def parse_gcs_location(value: str) -> Optional[Tuple[str, str]]:
    """Parse gs://bucket/blob or bucket/blob -> (bucket, blob)."""
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


def mb(size_bytes: int) -> float:
    return round(size_bytes / (1024 * 1024), 3)


def kb(size_bytes: int) -> float:
    return round(size_bytes / 1024, 3)


def derive_zip_from_video(bucket: str, video_blob: str) -> Tuple[str, str]:
    base_no_ext = os.path.splitext(video_blob)[0]
    return bucket, base_no_ext + "_metadata.zip"


def get_blob_size_bytes(storage: GCPStorageServices, bucket: str, blob: str) -> Optional[int]:
    bkt = storage.client.bucket(bucket)
    obj = bkt.get_blob(blob)
    if obj is None:
        return None
    return int(obj.size or 0)


def build_formula(status_field: str, status_value: str, imu_issue_field: str) -> str:
    """
    Airtable checkbox false often appears as blank, so treat blank as false.

    AND(
      {status}='successfully_processed',
      OR(IS_BLANK({imu_issue}), {imu_issue}=0)
    )
    """
    return (
        f"AND("
        f"{{{status_field}}}='{status_value}',"
        f"OR({{{imu_issue_field}}}=0, {{{imu_issue_field}}}=BLANK())"
        f")"
    )


@dataclass
class Result:
    uniq_id: str
    ok: bool
    video_bucket: Optional[str] = None
    video_blob: Optional[str] = None
    zip_bucket: Optional[str] = None
    zip_blob: Optional[str] = None
    video_size_mb: Optional[float] = None
    metadata_size_kb: Optional[float] = None
    updated_fields: Optional[List[str]] = None
    err: Optional[str] = None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry_run", action="store_true", help="No Airtable updates; just print what would happen.")
    ap.add_argument("--max_records", type=int, default=0, help="0 means no limit; otherwise cap processing.")
    ap.add_argument("--out_json", default="", help="Optional path to write a JSON summary.")
    ap.add_argument("--out_dir", default="data/size_update", help="Folder to write summary JSON if --out_json not provided.")

    ap.add_argument("--status_field_name", default="status")
    ap.add_argument("--status_value", default="successfully_processed")
    ap.add_argument("--imu_issue_field_name", default="imu_issue")

    ap.add_argument("--video_location_field_name", default="gcp_storage_video_location")
    ap.add_argument("--zip_location_field_name", default="gcp_storage_zip_location")

    ap.add_argument("--video_size_field_name", default="video_size_mb")
    ap.add_argument("--metadata_size_field_name", default="metadata_size_kb")
    args = ap.parse_args()

    airtable = AirtableServices()
    storage = GCPStorageServices()

    formula = build_formula(args.status_field_name, args.status_value, args.imu_issue_field_name)
    print(f"Using Airtable formula:\n  {formula}\n")

    records = airtable.video_table.all(formula=formula)
    if args.max_records and args.max_records > 0:
        records = records[: args.max_records]
    print(f"Loaded {len(records)} Airtable records")

    results: List[Result] = []
    updated_count = 0
    skipped_no_sizes = 0
    failed = 0

    for rec in tqdm(records, desc="Update sizes"):
        uniq_id = rec.get("id") or "unknown_record_id"
        fields: Dict[str, Any] = rec.get("fields", {}) or {}

        try:
            # Storage video location
            vloc = fields.get(args.video_location_field_name)
            pv = parse_gcs_location(vloc) if isinstance(vloc, str) else None
            if not pv:
                raise RuntimeError(f"Missing/unparsable {args.video_location_field_name}: {vloc}")
            v_bucket, v_blob = pv

            # Zip location (or derive)
            zloc = fields.get(args.zip_location_field_name)
            pz = parse_gcs_location(zloc) if isinstance(zloc, str) else None
            if pz:
                z_bucket, z_blob = pz
            else:
                z_bucket, z_blob = derive_zip_from_video(v_bucket, v_blob)

            # Sizes from GCS
            v_bytes = get_blob_size_bytes(storage, v_bucket, v_blob)
            z_bytes = get_blob_size_bytes(storage, z_bucket, z_blob)

            v_mb = mb(v_bytes) if v_bytes is not None else None
            z_kb = kb(z_bytes) if z_bytes is not None else None

            payload: Dict[str, Any] = {}
            updated_fields: List[str] = []

            # Only update what we found (avoid clearing existing values)
            if v_mb is not None:
                payload[args.video_size_field_name] = v_mb
                updated_fields.append(args.video_size_field_name)
            if z_kb is not None:
                payload[args.metadata_size_field_name] = z_kb
                updated_fields.append(args.metadata_size_field_name)

            if not payload:
                skipped_no_sizes += 1
                results.append(Result(
                    uniq_id=uniq_id,
                    ok=True,
                    video_bucket=v_bucket,
                    video_blob=v_blob,
                    zip_bucket=z_bucket,
                    zip_blob=z_blob,
                    video_size_mb=v_mb,
                    metadata_size_kb=z_kb,
                    updated_fields=[],
                    err="No sizes found in GCS; skipped Airtable update",
                ))
                continue

            if args.dry_run:
                print(f"DRY_RUN {uniq_id}: update {payload} (video gs://{v_bucket}/{v_blob}, zip gs://{z_bucket}/{z_blob})")
            else:
                airtable.update_video_table_single_video(uniq_id, payload)
                updated_count += 1

            results.append(Result(
                uniq_id=uniq_id,
                ok=True,
                video_bucket=v_bucket,
                video_blob=v_blob,
                zip_bucket=z_bucket,
                zip_blob=z_blob,
                video_size_mb=v_mb,
                metadata_size_kb=z_kb,
                updated_fields=updated_fields,
            ))

        except (NotFound, Forbidden) as e:
            failed += 1
            results.append(Result(uniq_id=uniq_id, ok=False, err=f"GCS error: {type(e).__name__}: {e}"))
        except Exception as e:
            failed += 1
            results.append(Result(uniq_id=uniq_id, ok=False, err=str(e)))

    os.makedirs(args.out_dir, exist_ok=True)
    out_json = args.out_json or os.path.join(args.out_dir, "run_summary.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(
            {
                "formula": formula,
                "count_total": len(results),
                "count_updated": updated_count,
                "count_skipped_no_sizes": skipped_no_sizes,
                "count_failed": failed,
                "results": [r.__dict__ for r in results],
            },
            f,
            indent=2,
        )

    print("\nDone.")
    print(f"Updated: {updated_count}")
    print(f"Skipped (no sizes found): {skipped_no_sizes}")
    print(f"Failed: {failed}")
    print(f"Summary JSON: {out_json}")
    if not args.dry_run:
        print("\nIf Airtable complains about value types, set those fields to 'Number' (allow decimals).")


if __name__ == "__main__":
    main()
