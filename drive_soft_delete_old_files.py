import argparse
from datetime import datetime
import pytz
from tqdm import tqdm
from controllers import GoogleDriveDownloader
from airtable_services import airtable_services
from video import Video

from tqdm import tqdm
#python drive_soft_delete_old_files.py --days_old 60 --limit 20

def build_videos_for_trash(df, drive_service, limit: int | None = None, show_progress: bool = True):
    """
    Build Video objects for trashing.

    limit:
      - If provided, only prepare the first N rows of df
      - This prevents multi-hour prep during dry_run on 10k+ records
    """
    if df is None or df.empty:
        return [], []

    if limit is not None:
        df = df.head(limit).copy()

    videos = []
    errors = []

    iterator = df.itertuples(index=False)
    if show_progress:
        iterator = tqdm(iterator, total=len(df), desc="Resolve Drive file IDs", unit="video")

    for row in iterator:
        info = row._asdict() if hasattr(row, "_asdict") else dict(row)
        v = Video(video_info=info)

        # If Airtable already has file id/path, use them (fast path)
        if info.get("google_drive_file_id"):
            v.google_drive_file_id = info.get("google_drive_file_id")
            v.google_drive_file_path = info.get("google_drive_file_path")
            errors.append(None)
        else:
            # Fallback: resolve file id by searching Drive folders
            err = v.set_file_id_file_path(google_drive_service=drive_service)
            errors.append(err)

        videos.append(v)

    return videos, errors


def main():
    parser = argparse.ArgumentParser(description="Soft delete old Google Drive files (move to trash)")
    parser.add_argument("--days_old", type=int, default=180, help="Minimum age in days since pipeline_run_date")
    parser.add_argument("--dry_run", action="store_true", help="Report only; do not trash or update Airtable")
    parser.add_argument("--limit", type=int, default=None, help="Optional max number of files to trash")
    args = parser.parse_args()

    downloader = GoogleDriveDownloader()

    df = airtable_services.get_videos_for_drive_soft_delete(days_old=args.days_old, limit=args.limit)
    if df.empty:
        print("No eligible videos found.")
        return

    videos, lookup_errors = build_videos_for_trash(df, downloader.drive_service, limit=args.limit)
    if lookup_errors:
        # keep it lightweight; you can print or write to a log file if you want
        print(f"Drive lookup errors (count={len([e for e in lookup_errors if e])}).")

    result = downloader.soft_delete_old_drive_files(videos, dry_run=args.dry_run, limit=args.limit)

    tz = pytz.timezone("America/Los_Angeles")
    print(f"Run date: {datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print("=== Result Summary ===")
    for k in ["checked", "trashed", "skipped", "failed", "dry_run"]:
        print(f"{k}: {result.get(k)}")


if __name__ == "__main__":
    main()
