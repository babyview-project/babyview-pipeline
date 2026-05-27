"""Slack webhook notifications for pipeline runs.

Configure creds/slack_webhook.json:
  {"webhook_url": "https://hooks.slack.com/services/..."}
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

import pandas as pd

import settings
from status_types import VideoStatus

logger = logging.getLogger(__name__)

SUCCESS_STATUSES = {VideoStatus.PROCESSED, VideoStatus.REMOVED}
MAX_FAILURE_LINES = 12


def _load_webhook_url() -> str | None:
    try:
        with open(settings.slack_webhook_path, "r") as f:
            data = json.load(f)
        url = (data.get("webhook_url") or "").strip()
        return url or None
    except FileNotFoundError:
        logger.warning("Slack webhook not configured (%s missing)", settings.slack_webhook_path)
        return None
    except Exception as e:
        logger.warning("Failed to load Slack webhook: %s", e)
        return None


def send_slack_message(text: str) -> bool:
    webhook_url = _load_webhook_url()
    if not webhook_url:
        return False
    payload = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.status >= 400:
                logger.warning("Slack webhook returned status %s", resp.status)
                return False
        return True
    except urllib.error.URLError as e:
        logger.warning("Slack notification failed: %s", e)
        return False


def _format_filter_line(run_context: dict[str, Any] | None) -> str:
    if not run_context:
        return "• Filter: (none)"
    key = run_context.get("filter_key")
    value = run_context.get("filter_value")
    if key:
        return f"• Filter: `{key}` = `{value}`"
    return "• Filter: default Airtable query"


def summarize_tracking_dataframe(df: pd.DataFrame) -> list[str]:
    lines = [f"• *Queried from Airtable:* {len(df)} videos"]
    if df.empty:
        return lines

    if "subject_id" in df.columns:
        subjects = df["subject_id"].dropna().nunique()
        lines.append(f"• *Subjects:* {subjects}")

    if "status" in df.columns:
        status_counts = df["status"].fillna("(blank)").value_counts()
        status_parts = [f"{k}: {v}" for k, v in status_counts.head(8).items()]
        lines.append(f"• *By status:* {', '.join(status_parts)}")

    if "dataset" in df.columns:
        dataset_counts = df["dataset"].fillna("(blank)").value_counts()
        dataset_parts = [f"{k}: {v}" for k, v in dataset_counts.head(6).items()]
        lines.append(f"• *By dataset:* {', '.join(dataset_parts)}")

    return lines


def format_run_started_message(
    df: pd.DataFrame,
    *,
    run_context: dict[str, Any] | None = None,
    videos_to_process: int | None = None,
) -> str:
    lines = [
        ":rocket: *BabyView Pipeline — Run Started*",
        f"• *Time:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
    ]
    lines.extend(summarize_tracking_dataframe(df))
    if videos_to_process is not None:
        lines.append(f"• *Resolved for processing:* {videos_to_process} videos")
    if run_context:
        lines.append(_format_filter_line(run_context))
        lines.append(f"• *Download source:* `{run_context.get('download_source', 'google_drive')}`")
        if run_context.get("limit"):
            lines.append(f"• *Limit:* {run_context['limit']}")
        if run_context.get("dry_run"):
            lines.append("• *Mode:* `dry_run` (no processing)")
    return "\n".join(lines)


def _count_outcomes(outcomes: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in outcomes:
        status = row.get("status") or "unknown"
        counts[str(status)] += 1
    return dict(counts)


def _failure_lines(outcomes: list[dict[str, Any]]) -> list[str]:
    failures = [
        o for o in outcomes
        if o.get("status") not in SUCCESS_STATUSES and o.get("status")
    ]
    if not failures:
        return ["• *Failures:* none"]

    by_status: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in failures:
        by_status[str(row.get("status") or "unknown")].append(row)

    lines = [f"• *Failed:* {len(failures)} videos"]
    shown = 0
    for status, rows in sorted(by_status.items(), key=lambda x: -len(x[1])):
        lines.append(f"  ◦ `{status}` ({len(rows)})")
        for row in rows:
            if shown >= MAX_FAILURE_LINES:
                lines.append(f"  ◦ … and {len(failures) - shown} more (see GCS logs)")
                return lines
            vid = row.get("video_id") or "?"
            msg = (row.get("message") or "").strip()
            if len(msg) > 120:
                msg = msg[:117] + "..."
            detail = f"`{vid}`"
            if msg:
                detail += f" — {msg}"
            lines.append(f"    - {detail}")
            shown += 1
    return lines


def format_run_finished_message(
    *,
    queried_count: int,
    outcomes: list[dict[str, Any]],
    logs: dict[str, Any],
    run_context: dict[str, Any] | None = None,
    duration_sec: float | None = None,
    log_object: str | None = None,
) -> str:
    status_counts = _count_outcomes(outcomes)
    processed = status_counts.get(VideoStatus.PROCESSED, 0)
    deleted = status_counts.get(VideoStatus.REMOVED, 0)
    failed = sum(c for s, c in status_counts.items() if s not in SUCCESS_STATUSES)

    lines = [
        ":white_check_mark: *BabyView Pipeline — Run Finished*",
        f"• *Time:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"• *Queried:* {queried_count} | *Processed:* {processed} | *Deleted:* {deleted} | *Failed:* {failed}",
    ]

    if status_counts:
        summary_parts = [f"{k}: {v}" for k, v in sorted(status_counts.items(), key=lambda x: -x[1])]
        lines.append(f"• *Outcome breakdown:* {', '.join(summary_parts[:10])}")

    lines.extend(_failure_lines(outcomes))

    step_fail_keys = sorted(k for k in logs if k.endswith("_fail"))
    if step_fail_keys:
        step_parts = [f"{k}: {len(logs[k])}" for k in step_fail_keys if logs[k]]
        if step_parts:
            lines.append(f"• *Step errors:* {', '.join(step_parts)}")

    if logs.get("unexpected_error"):
        lines.append(f"• *Unexpected errors:* {len(logs['unexpected_error'])}")
    if logs.get("general_error"):
        lines.append(f"• *General errors:* {len(logs['general_error'])}")

    if duration_sec is not None:
        mins, secs = divmod(int(duration_sec), 60)
        hours, mins = divmod(mins, 60)
        if hours:
            lines.append(f"• *Duration:* {hours}h {mins}m {secs}s")
        elif mins:
            lines.append(f"• *Duration:* {mins}m {secs}s")
        else:
            lines.append(f"• *Duration:* {secs}s")

    if log_object:
        lines.append(f"• *GCS log:* `{log_object}`")

    if run_context:
        lines.append(_format_filter_line(run_context))

    return "\n".join(lines)


def notify_run_started(
    df: pd.DataFrame,
    *,
    run_context: dict[str, Any] | None = None,
    videos_to_process: int | None = None,
) -> None:
    message = format_run_started_message(
        df, run_context=run_context, videos_to_process=videos_to_process
    )
    send_slack_message(message)


def notify_run_finished(
    *,
    queried_count: int,
    outcomes: list[dict[str, Any]],
    logs: dict[str, Any],
    run_context: dict[str, Any] | None = None,
    duration_sec: float | None = None,
    log_object: str | None = None,
) -> None:
    message = format_run_finished_message(
        queried_count=queried_count,
        outcomes=outcomes,
        logs=logs,
        run_context=run_context,
        duration_sec=duration_sec,
        log_object=log_object,
    )
    send_slack_message(message)
