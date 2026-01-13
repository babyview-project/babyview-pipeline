# databrary_patch_missing_source_dates.py
import argparse

from databrary_client import DatabraryClient

#python databrary_patch_missing_source_dates.py --volume_id 1882 --dry_run
#python databrary_patch_missing_source_dates.py --volume_id 1882 --session_id 77713 --dry_run

def main():
    parser = argparse.ArgumentParser(
        description="Scan Databrary volume/session and PATCH files missing source_date; update Airtable."
    )
    parser.add_argument("--volume_id", type=int, required=True, help="Databrary volume id")
    parser.add_argument("--session_id", type=int, default=None, help="Optional Databrary session id to limit scope")
    parser.add_argument("--dry_run", action="store_true", help="Do not PATCH or update Airtable; only report")
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on number of patches")
    parser.add_argument("--file_progress", action="store_true",
                        help="Show per-session file progress bars (slower but more detailed)")

    args = parser.parse_args()

    dc = DatabraryClient()
    summary = dc.patch_missing_source_dates(
        volume_id=args.volume_id,
        session_id=args.session_id,
        dry_run=args.dry_run,
        limit=args.limit,
        show_progress=True,
        show_file_progress=args.file_progress,
    )

    print("=== Summary ===")
    for k, v in summary.items():
        if k == "errors":
            continue
        print(f"{k}: {v}")

    if summary.get("errors"):
        print("\n=== Errors (first 50) ===")
        for e in summary["errors"][:50]:
            print(e)


if __name__ == "__main__":
    main()