# databrary_patch_missing_source_dates.py
import argparse

from airtable_services import airtable_services
from databrary_client import DatabraryClient

# python databrary_patch_missing_source_dates.py --volume_id 1882 --dry_run
# python databrary_patch_missing_source_dates.py --volume_id 1882 --session_id 77713 --dry_run
# python databrary_patch_missing_source_dates.py --volume_id 1882 \
#   --release 2026.1 --release-mode only --require-upload-date --dry_run


def _release_formula_part(release_name: str, release_mode: str) -> str:
    if release_mode == "only":
        return f"ARRAYJOIN({{release}}) = '{release_name}'"
    return f"FIND('{release_name}', ARRAYJOIN({{release}}))"


def _fetch_allowed_record_ids(
    *,
    release: str | None,
    release_mode: str,
    require_upload_date: bool,
) -> set[str]:
    """Airtable video record IDs matching optional release / upload-date filters."""
    parts: list[str] = []
    if release:
        parts.append(_release_formula_part(release, release_mode))
    if require_upload_date:
        parts.append("{databrary_upload_date}")
    if not parts:
        raise ValueError("Airtable allowlist requested but no release/upload-date filters set")

    formula = parts[0] if len(parts) == 1 else "AND(" + ", ".join(parts) + ")"
    print(f"[INFO] Airtable allowlist formula: {formula}")
    records = airtable_services.video_table.all(formula=formula)
    ids = {r["id"] for r in records if r.get("id")}
    print(f"[INFO] Airtable allowlist size: {len(ids)}")
    return ids


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
    parser.add_argument(
        "--release",
        type=str,
        default=None,
        help='Optional Airtable release tag filter (e.g. "2026.1")',
    )
    parser.add_argument(
        "--release-mode",
        choices=("has", "only"),
        default="has",
        help="has = includes release tag; only = sole release tag (default: has)",
    )
    parser.add_argument(
        "--require-upload-date",
        action="store_true",
        help="Only consider Airtable videos with non-empty databrary_upload_date",
    )

    args = parser.parse_args()

    if args.release_mode != "has" and not args.release:
        parser.error("--release-mode requires --release")

    allowed_record_ids = None
    if args.release or args.require_upload_date:
        allowed_record_ids = _fetch_allowed_record_ids(
            release=args.release,
            release_mode=args.release_mode,
            require_upload_date=args.require_upload_date,
        )
        if not allowed_record_ids:
            print("[INFO] No Airtable videos matched filters; nothing to patch.")
            return

    dc = DatabraryClient()
    summary = dc.patch_missing_source_dates(
        volume_id=args.volume_id,
        session_id=args.session_id,
        dry_run=args.dry_run,
        limit=args.limit,
        show_progress=True,
        show_file_progress=args.file_progress,
        allowed_record_ids=allowed_record_ids,
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
