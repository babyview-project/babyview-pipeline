#!/usr/bin/env python3
"""
checking_metadata_from_file_update_airtable.py

Reads an input .txt file (one line per record/path), extracts Airtable record IDs (uniq_id),
checks GCS for corresponding video + metadata objects to compute sizes, then UPDATES Airtable:

- video_size_mb (float, MB)
- metadata_size_kb (float, KB)
- imu_issue (bool) -> True if the uniq_id appears in the input txt (i.e., processed by this script)

Usage:
  python checking_metadata_from_file_update_airtable.py --txt_path /path/to/input.txt

Optional:
  --buckets babyview_main_storage,babyview_bing_storage
  --gcs_limit 20000
  --dry_run   (print intended updates without writing to Airtable)

Notes:
- GCS lookup strategy:
  1) Find blobs whose name contains uniq_id (match_glob if supported; else prefix tries)
  2) Pick metadata (prefer *_metadata.zip) and video (prefer video extensions) from matches
  3) If video not found by uniq_id, list the metadata folder and pick the largest likely video there
"""

from __future__ import annotations

import argparse
import inspect
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from tqdm import tqdm
from google.api_core.exceptions import Forbidden, NotFound

from airtable_services import AirtableServices
from gcp_storage_services import GCPStorageServices


# --------- uniq_id extraction ---------
UNIQ_ID_FROM_METADATA_ZIP_RE = re.compile(r"_(rec[a-zA-Z0-9]{14})_metadata\.zip", re.IGNORECASE)
UNIQ_ID_RE = re.compile(r"(rec[a-zA-Z0-9]{14})")


def extract_unique_id(line: str) -> Optional[str]:
    """Extract Airtable record id from a line."""
    m = UNIQ_ID_FROM_METADATA_ZIP_RE.search(line)
    if m:
        return m.group(1)
    m = UNIQ_ID_RE.search(line)
    return m.group(1) if m else None


# --------- GCS helpers ---------
VIDEO_EXTS = {".mp4", ".mov", ".lrv", ".mkv", ".avi"}
META_EXTS = {".zip", ".json", ".csv", ".txt", ".pkl", ".parquet", ".gz"}


@dataclass
class GcsBlobInfo:
    name: str
    size_bytes: int


def mb(size_bytes: int) -> float:
    return round(size_bytes / (1024 * 1024), 3)


def kb(size_bytes: int) -> float:
    return round(size_bytes / 1024, 3)


def dirname_prefix(blob_name: str) -> str:
    d = os.path.dirname(blob_name)
    return (d + "/") if d else ""


def pick_video(blobs: List[GcsBlobInfo]) -> Optional[GcsBlobInfo]:
    vids = [b for b in blobs if os.path.splitext(b.name.lower())[1] in VIDEO_EXTS]
    return max(vids, key=lambda x: x.size_bytes) if vids else None


def pick_metadata(blobs: List[GcsBlobInfo]) -> Optional[GcsBlobInfo]:
    candidates: List[GcsBlobInfo] = []
    for b in blobs:
        name_l = b.name.lower()
        ext = os.path.splitext(name_l)[1]
        if "metadata" in name_l:
            candidates.append(b)
        elif ext in META_EXTS:
            candidates.append(b)

    if not candidates:
        return None

    # Strong preference for *_metadata.zip
    meta_zip = [b for b in candidates if b.name.lower().endswith("_metadata.zip")]
    if meta_zip:
        return max(meta_zip, key=lambda x: x.size_bytes)

    explicit = [b for b in candidates if "metadata" in b.name.lower()]
    if explicit:
        return max(explicit, key=lambda x: x.size_bytes)

    return max(candidates, key=lambda x: x.size_bytes)


def list_blobs_containing_id(
    storage: GCPStorageServices,
    bucket_name: str,
    uniq_id: str,
    *,
    limit: int,
) -> List[GcsBlobInfo]:
    """
    Find blobs whose name contains uniq_id.
    Prefer match_glob when supported; otherwise use prefix tries.
    """
    client = storage.client
    out: List[GcsBlobInfo] = []

    # Preferred: match_glob (fast, no prefix guessing)
    try:
        sig = inspect.signature(client.list_blobs)
        if "match_glob" in sig.parameters:
            it = client.list_blobs(bucket_name, match_glob=f"**/*{uniq_id}*")
            for i, blob in enumerate(it):
                if i >= limit:
                    break
                out.append(GcsBlobInfo(name=blob.name, size_bytes=int(blob.size or 0)))
            return out
    except Exception:
        pass

    # Fallback: try likely prefixes first (avoid whole-bucket scan)
    prefix_candidates = [
        "imu_processing/",
        "raw/",
        "processed/",
        "videos/",
        "zip/",
        "",  # last resort (can be expensive)
    ]

    for pref in prefix_candidates:
        it = client.list_blobs(bucket_name, prefix=pref)
        for i, blob in enumerate(it):
            if i >= limit:
                break
            if uniq_id in blob.name:
                out.append(GcsBlobInfo(name=blob.name, size_bytes=int(blob.size or 0)))
        if out:
            return out

    return out


def list_blobs_in_prefix(
    storage: GCPStorageServices,
    bucket_name: str,
    prefix: str,
    *,
    limit: int,
) -> List[GcsBlobInfo]:
    client = storage.client
    out: List[GcsBlobInfo] = []
    it = client.list_blobs(bucket_name, prefix=prefix)
    for i, blob in enumerate(it):
        if i >= limit:
            break
        out.append(GcsBlobInfo(name=blob.name, size_bytes=int(blob.size or 0)))
    return out


def find_sizes_in_gcs(
    storage: GCPStorageServices,
    uniq_id: str,
    buckets: List[str],
    gcs_limit: int,
) -> Dict[str, Any]:
    """
    Returns:
      {
        bucket_used: str|None,
        video_blob: str|None,
        video_size_mb: float|None,
        metadata_blob: str|None,
        metadata_size_kb: float|None,
        gcs_error: str|None,
      }
    """
    bucket_used: Optional[str] = None
    gcs_error: Optional[str] = None

    video_match: Optional[GcsBlobInfo] = None
    meta_match: Optional[GcsBlobInfo] = None

    try:
        for bkt in buckets:
            matches = list_blobs_containing_id(storage, bkt, uniq_id, limit=gcs_limit)
            if not matches:
                continue

            bucket_used = bkt
            meta_match = pick_metadata(matches)
            video_match = pick_video(matches)

            # If video not found by uniq_id, scan the metadata folder (or any match folder)
            if not video_match:
                anchor = meta_match or matches[0]
                pref = dirname_prefix(anchor.name)
                if pref:
                    folder_blobs = list_blobs_in_prefix(storage, bkt, pref, limit=gcs_limit)
                    video_match = pick_video(folder_blobs) or video_match
                    meta_match = meta_match or pick_metadata(folder_blobs)

            # If metadata still missing but video exists, scan video folder
            if not meta_match and video_match:
                pref = dirname_prefix(video_match.name)
                if pref:
                    folder_blobs = list_blobs_in_prefix(storage, bkt, pref, limit=gcs_limit)
                    meta_match = pick_metadata(folder_blobs)

            break  # stop after first bucket with any matches

        if not bucket_used:
            gcs_error = f"Not found in buckets: {buckets}"

    except (NotFound, Forbidden) as e:
        gcs_error = f"GCS access error: {type(e).__name__}: {e}"
    except Exception as e:
        gcs_error = f"GCS error: {type(e).__name__}: {e}"

    return {
        "bucket_used": bucket_used,
        "video_blob": (video_match.name if video_match else None),
        "video_size_mb": (mb(video_match.size_bytes) if video_match else None),
        "metadata_blob": (meta_match.name if meta_match else None),
        "metadata_size_kb": (kb(meta_match.size_bytes) if meta_match else None),
        "gcs_error": gcs_error,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--txt_path", required=True, help="Input txt file (one record/path per line)")
    ap.add_argument(
        "--buckets",
        default="babyview_main_storage,babyview_bing_storage",
        help="Comma-separated bucket names to search (in order)",
    )
    ap.add_argument(
        "--gcs_limit",
        type=int,
        default=20000,
        help="Max blobs to iterate per list operation (safety cap)",
    )
    ap.add_argument(
        "--dry_run",
        action="store_true",
        help="Print intended Airtable updates without writing",
    )
    args = ap.parse_args()

    buckets = [b.strip() for b in args.buckets.split(",") if b.strip()]

    airtable = AirtableServices()
    storage = GCPStorageServices()

    with open(args.txt_path, "r", encoding="utf-8") as f:
        lines = [ln.strip() for ln in f.readlines()]
    lines = [ln for ln in lines if ln and not ln.lstrip().startswith("#")]

    uniq_ids: List[str] = []
    for line in lines:
        uid = extract_unique_id(line)
        if uid:
            uniq_ids.append(uid)

    # de-dupe but keep order
    seen = set()
    uniq_ids = [u for u in uniq_ids if not (u in seen or seen.add(u))]

    print(f"Found {len(uniq_ids)} uniq_ids in input file.")

    updated = 0
    failed = 0

    for uniq_id in tqdm(uniq_ids, desc="Updating Airtable"):
        gcs_info = find_sizes_in_gcs(
            storage=storage,
            uniq_id=uniq_id,
            buckets=buckets,
            gcs_limit=args.gcs_limit,
        )

        # imu_issue is True because uniq_id appears in the input txt
        payload = {
            "video_size_mb": gcs_info["video_size_mb"],
            "metadata_size_kb": gcs_info["metadata_size_kb"],
            "imu_issue": True,
        }

        if args.dry_run:
            print(
                f"DRY_RUN uniq_id={uniq_id} bucket={gcs_info['bucket_used']} "
                f"video_mb={payload['video_size_mb']} meta_kb={payload['metadata_size_kb']} "
                f"gcs_error={gcs_info['gcs_error']}"
            )
            updated += 1
            continue

        try:
            airtable.update_video_table_single_video(uniq_id, payload)
            updated += 1
        except Exception as e:
            failed += 1
            print(f"FAILED update uniq_id={uniq_id}: {type(e).__name__}: {e}")

    print("\nDone.")
    print(f"Updated: {updated}")
    print(f"Failed : {failed}")


if __name__ == "__main__":
    main()
