# databrary_backfill.py

import os
import re
import time
from typing import Any, List

from datetime import datetime
import pytz
from tqdm import tqdm

from gcp_storage_services import GCPStorageServices
from airtable_services import airtable_services
from video import Video
from databrary_client import DatabraryClient
from slack_notifier import notify_databrary_finished, notify_databrary_started

storage = GCPStorageServices()
dc = DatabraryClient()

# Airtable / pipeline store GCS video as: "<bucket_name>_storage/<subject>/<file>.mp4"
_GCS_STORAGE_RE = re.compile(
    r"^[A-Za-z0-9._-]+_storage/.+\.mp4$",
    re.IGNORECASE,
)


def _is_valid_gcs_storage_location(loc: Any) -> bool:
    """Non-empty, not an error string, and looks like a pipeline GCS storage URL."""
    if not loc or not isinstance(loc, str):
        return False
    s = loc.strip()
    if not s:
        return False
    upper = s.upper()
    if upper.startswith("ERROR") or "ERROR:" in upper:
        return False
    if "_storage/" not in s:
        return False
    bucket_part, _, rest = s.partition("/")
    if not bucket_part.endswith("_storage") or not rest:
        return False
    return bool(_GCS_STORAGE_RE.match(s))


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


def _base_gcs_formula_parts() -> List[str]:
    """Shared selection filters: valid non-empty GCS location, no error text."""
    return [
        "{gcp_storage_video_location}",
        'NOT(FIND("ERROR", UPPER({gcp_storage_video_location})))',
        'FIND("_storage/", {gcp_storage_video_location})',
    ]


def _release_formula_part(release_name: str, release_mode: str) -> str:
    """
    release_mode:
      - has:  videos that include this release tag (may have others)
      - only: videos whose only release tag is this one
    """
    # Airtable LEN() on linked records is unreliable; ARRAYJOIN equality means
    # the joined primary-field names are exactly this single release.
    if release_mode == "only":
        return f"ARRAYJOIN({{release}}) = '{release_name}'"
    return f"FIND('{release_name}', ARRAYJOIN({{release}}))"


def select_video_ids_for_databrary(
    *,
    release: str | None = None,
    release_mode: str = "has",
    subject_ids: List[str] | None = None,
    unique_video_ids: List[str] | None = None,
    status_test: int | None = None,
    limit: int | None = None,
) -> List[str]:
    """
    Select Airtable video record IDs. All provided filters are AND'd together,
    plus the base GCS location constraints (no status=successfully_processed).
    """
    if release_mode not in ("has", "only"):
        raise ValueError(f"release_mode must be 'has' or 'only', got {release_mode!r}")

    formula_parts = list(_base_gcs_formula_parts())

    if release:
        formula_parts.append(_release_formula_part(release, release_mode))

    if subject_ids:
        formula_parts.append(airtable_services._build_subject_filter(subject_ids))

    if status_test is not None:
        formula_parts.append(f"{{status_test}} = {int(status_test)}")

    if unique_video_ids:
        id_filter = "OR(" + ", ".join([f"RECORD_ID() = '{rid}'" for rid in unique_video_ids]) + ")"
        formula_parts.append(id_filter)

    formula = "AND(" + ", ".join(formula_parts) + ")"
    print(f"[INFO] Airtable formula: {formula}")

    try:
        records = airtable_services.video_table.all(formula=formula)
    except Exception as e:
        print(f"[ERROR] Failed to fetch records: {e}")
        return []

    video_ids: List[str] = []
    for rec in records:
        fields = rec.get("fields", {})
        loc = fields.get("gcp_storage_video_location")
        if not _is_valid_gcs_storage_location(loc):
            print(
                f"[FILTER] skip {rec.get('id')}: invalid gcp_storage_video_location={loc!r}"
            )
            continue
        if rec.get("id"):
            video_ids.append(rec["id"])

    if limit is not None:
        video_ids = video_ids[:limit]

    print(f"[INFO] Selected {len(video_ids)} candidate videos after filters.")
    return video_ids


def backfill_databrary_for_video_ids(
    video_record_ids: List[str],
    dry_run: bool = False,
    show_progress: bool = True,
) -> dict[str, Any]:
    """
    Backfill Databrary uploads for specific Airtable video record IDs.

    Steps per record:
      - read Airtable row
      - download compressed mp4 from GCS to temp
      - build Video object with that local path
      - call DatabraryClient.upload_video(video)
      - remove local file

    Returns a summary dict for Slack / logging.
    """
    summary: dict[str, Any] = {
        "candidates": len(video_record_ids),
        "success": 0,
        "failed": 0,
        "skipped": 0,
        "errors": [],
    }

    iterator = video_record_ids
    if show_progress:
        iterator = tqdm(video_record_ids, desc="Databrary backfill", unit="video")

    for vid_id in iterator:
        print(f"=== Backfill Databrary for {vid_id} ===")
        try:
            record = airtable_services.video_table.get(vid_id)
        except Exception as e:
            msg = f"Failed to fetch Airtable record: {e}"
            print(f"[ERROR] {vid_id}: {msg}")
            summary["failed"] += 1
            summary["errors"].append(f"{vid_id}: {msg}")
            continue

        fields = record.get("fields", {})
        gcp_storage_video_location = fields.get("gcp_storage_video_location")
        databrary_upload_date = fields.get("databrary_upload_date")
        if databrary_upload_date:
            print(f"[SKIP] {vid_id}: it has been uploaded on {databrary_upload_date}")
            summary["skipped"] += 1
            continue

        if not _is_valid_gcs_storage_location(gcp_storage_video_location):
            msg = f"DOWNLOAD: invalid gcp_storage_video_location: {gcp_storage_video_location!r}"
            print(f"[SKIP] {vid_id}: {msg}")
            summary["skipped"] += 1
            summary["errors"].append(f"{vid_id}: {msg}")
            continue

        if dry_run:
            print(f"[DRY_RUN] {vid_id}: would download from {gcp_storage_video_location}")
            summary["skipped"] += 1
            continue

        # 1) Download compressed mp4 from GCS to temp
        local_path, dl_err = _download_from_gcs_to_temp(gcp_storage_video_location)
        if dl_err:
            print(f"[ERROR] {vid_id}: {dl_err}")
            _mark_airtable_error(vid_id, dl_err)
            summary["failed"] += 1
            summary["errors"].append(f"{vid_id}: {dl_err}")
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
            summary["failed"] += 1
            summary["errors"].append(f"{vid_id}: {msg}")
            try:
                os.remove(local_path)
            except Exception:
                pass
            continue
        print(f"Video Info being uploaded: {video.to_dict()}")

        # 3) Call DatabraryClient (this will ALWAYS write databrary_* fields)
        status_url, error_log = dc.upload_video(video)
        if error_log:
            print(f"[Databrary] {vid_id}: errors -> {' | '.join(error_log)}")
            summary["failed"] += 1
            summary["errors"].append(f"{vid_id}: {' | '.join(error_log)}")
        else:
            print(f"[Databrary] {vid_id}: success -> {status_url}")
            summary["success"] += 1

        # 4) Clean up local file
        try:
            os.remove(local_path)
        except Exception:
            pass

    return summary


def run_databrary_upload(
    *,
    release: str | None = None,
    release_mode: str = "has",
    subject_ids: List[str] | None = None,
    unique_video_ids: List[str] | None = None,
    status_test: int | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    show_progress: bool = True,
) -> dict[str, Any]:
    """Select candidates (AND filters), Slack start/finish, then upload."""
    filters = {
        "release": release,
        "release_mode": release_mode if release else None,
        "subject_id": subject_ids,
        "unique_video_id": unique_video_ids,
        "status_test": status_test,
        "limit": limit,
    }

    # If only explicit IDs are given with no other selectors, still AND with base GCS
    # filters via select_video_ids_for_databrary.
    video_ids = select_video_ids_for_databrary(
        release=release,
        release_mode=release_mode,
        subject_ids=subject_ids or None,
        unique_video_ids=unique_video_ids or None,
        status_test=status_test,
        limit=limit,
    )

    notify_databrary_started(
        candidate_count=len(video_ids),
        filters=filters,
        dry_run=dry_run,
    )
    started = time.time()
    if not video_ids:
        summary = {
            "candidates": 0,
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "errors": ["No candidate videos matched filters."],
        }
    else:
        summary = backfill_databrary_for_video_ids(
            video_ids,
            dry_run=dry_run,
            show_progress=show_progress,
        )
    notify_databrary_finished(
        summary=summary,
        filters=filters,
        duration_sec=time.time() - started,
        dry_run=dry_run,
    )
    return summary


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Databrary backfill from GCS. Filters are AND'd: "
            "--release/--release-mode, --subject_id, --unique_video_id, --status_test, "
            "plus base GCS location checks (non-empty, no ERROR, valid *_storage/... .mp4)."
        )
    )
    parser.add_argument(
        "--unique_video_id",
        action="append",
        default=None,
        help="Airtable video record ID (repeatable); AND'd with other filters",
    )
    parser.add_argument(
        "--release",
        type=str,
        default=None,
        help='Release name tag (e.g. "2026.1"); use with --release-mode',
    )
    parser.add_argument(
        "--release-mode",
        choices=("has", "only"),
        default="has",
        help=(
            "has = videos that include this release tag (default); "
            "only = videos whose only release tag is this one"
        ),
    )
    parser.add_argument(
        "--subject_id",
        action="append",
        default=None,
        help="Participant subject_id (e.g. S00240001); repeatable; AND'd with other filters",
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Select by base GCS filters only (and any other filters you pass)",
    )
    parser.add_argument(
        "--status_test",
        type=int,
        default=None,
        help="Optional Airtable status_test value; AND'd with other filters",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on number of selected videos",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Report only; do not download/upload or update Airtable.",
    )
    parser.add_argument(
        "--no_progress",
        action="store_true",
        help="Disable progress bar output.",
    )

    args = parser.parse_args()

    has_selector = any(
        [
            args.unique_video_id,
            args.release,
            args.subject_id,
            args.auto,
            args.status_test is not None,
        ]
    )
    if not has_selector:
        parser.error(
            "Provide at least one selector: --auto, --release, --subject_id, "
            "--unique_video_id, and/or --status_test"
        )

    if args.release_mode != "has" and not args.release:
        parser.error("--release-mode requires --release")

    run_databrary_upload(
        release=args.release,
        release_mode=args.release_mode,
        subject_ids=args.subject_id,
        unique_video_ids=args.unique_video_id,
        status_test=args.status_test,
        limit=args.limit,
        dry_run=args.dry_run,
        show_progress=not args.no_progress,
    )
