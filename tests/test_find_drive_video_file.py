"""Tests for Video._find_drive_video_file and _scan_folder_for_drive_video.

These tests do NOT hit Google Drive. They mock `files().list().execute()` and
exercise every tier of the lookup waterfall plus the disambiguation rule
(exact name beats '<base> (N).<ext>' variants).

Run with:  pytest -q tests/test_find_drive_video_file.py
"""

from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from video import Video


FILE_NAME = "GX010067.MP4"
FOLDER_ID = "FOLDER_ID_FAKE"
LIST_KWARGS = {
    "driveId": "drive123",
    "corpora": "drive",
    "includeItemsFromAllDrives": True,
    "supportsAllDrives": True,
    "fields": "files(id, name)",
}


def _make_video() -> Video:
    """Bypass __init__ so we don't need full Airtable-style metadata."""
    return Video.__new__(Video)


def _make_drive_service(call_handler):
    """Build a mock with files().list(**kwargs).execute() returning whatever
    `call_handler(call_index, q, kwargs)` returns. Records every call.
    """
    calls = []

    def _list(**kwargs):
        idx = len(calls)
        q = kwargs.get("q", "")
        calls.append({"q": q, "kwargs": kwargs})
        result = call_handler(idx, q, kwargs)

        execute_mock = MagicMock()
        execute_mock.execute.return_value = result
        return execute_mock

    files_mock = MagicMock()
    files_mock.list.side_effect = _list

    service = MagicMock()
    service.files.return_value = files_mock
    service._calls = calls
    return service


# --------------------------------------------------------------------------- #
# Tier 1: exact match short-circuits                                          #
# --------------------------------------------------------------------------- #


def test_tier1_exact_match_returns_immediately():
    def handler(idx, q, kwargs):
        assert idx == 0, "tier 1 should be the only call"
        assert "name = 'GX010067.MP4'" in q
        return {"files": [{"id": "FILE_A", "name": "GX010067.MP4"}]}

    service = _make_drive_service(handler)
    v = _make_video()

    item = v._find_drive_video_file(service, FOLDER_ID, FILE_NAME, LIST_KWARGS)

    assert item == {"id": "FILE_A", "name": "GX010067.MP4"}
    assert len(service._calls) == 1


# --------------------------------------------------------------------------- #
# Tier 2: name contains, exact-name match wins                                #
# --------------------------------------------------------------------------- #


def test_tier2_returns_when_exact_name_present_in_contains_results():
    def handler(idx, q, kwargs):
        if idx == 0:
            assert "name = 'GX010067.MP4'" in q
            return {"files": []}
        if idx == 1:
            assert "name contains 'GX010067'" in q
            # exact-name file is mixed in with a suffix sibling and noise
            return {
                "files": [
                    {"id": "noise", "name": "GX010067_extra.MP4"},
                    {"id": "FILE_SUFFIX", "name": "GX010067 (1).MP4"},
                    {"id": "FILE_EXACT", "name": "GX010067.MP4"},
                ]
            }
        pytest.fail(f"tier 3 should not run, got call idx={idx}")

    service = _make_drive_service(handler)
    v = _make_video()

    item = v._find_drive_video_file(service, FOLDER_ID, FILE_NAME, LIST_KWARGS)

    assert item["id"] == "FILE_EXACT"
    assert len(service._calls) == 2  # tier 1 + tier 2 only


def test_tier2_only_suffix_falls_through_to_tier3():
    # Tier 2 sees only a suffix variant -> must NOT return; must run tier 3
    # so that a possibly-stale-indexed exact-name file isn't missed.
    def handler(idx, q, kwargs):
        if idx == 0:
            return {"files": []}
        if idx == 1:
            assert "name contains 'GX010067'" in q
            return {"files": [{"id": "FILE_SUFFIX", "name": "GX010067 (1).MP4"}]}
        if idx == 2:
            # tier 3: full folder scan, no name filter
            assert "name contains" not in q
            assert "name =" not in q
            assert "trashed = false" in q
            assert kwargs["fields"] == "nextPageToken, files(id, name)"
            return {"files": [{"id": "FILE_EXACT", "name": "GX010067.MP4"}]}
        pytest.fail("unexpected extra call")

    service = _make_drive_service(handler)
    v = _make_video()

    item = v._find_drive_video_file(service, FOLDER_ID, FILE_NAME, LIST_KWARGS)

    # Exact-name file found by tier 3 must win over tier-2's suffix candidate.
    assert item["id"] == "FILE_EXACT"
    assert len(service._calls) == 3


def test_tier3_returns_suffix_when_no_exact_exists():
    # Truly only a suffix variant exists (rename never happened). Tier 3 scans
    # the whole folder and, finding no exact-name match, returns the suffix.
    def handler(idx, q, kwargs):
        if idx == 0:
            return {"files": []}
        if idx == 1:
            return {"files": [{"id": "FILE_SUFFIX", "name": "GX010067 (1).MP4"}]}
        if idx == 2:
            return {
                "files": [
                    {"id": "noise", "name": "GX010068.MP4"},
                    {"id": "FILE_SUFFIX", "name": "GX010067 (1).MP4"},
                ]
            }
        pytest.fail("unexpected extra call")

    service = _make_drive_service(handler)
    v = _make_video()

    item = v._find_drive_video_file(service, FOLDER_ID, FILE_NAME, LIST_KWARGS)

    assert item["id"] == "FILE_SUFFIX"


# --------------------------------------------------------------------------- #
# Tier 3 disambiguation across pages                                          #
# --------------------------------------------------------------------------- #


def test_tier3_exact_in_later_page_beats_suffix_in_earlier_page():
    def handler(idx, q, kwargs):
        if idx == 0:
            return {"files": []}
        if idx == 1:
            return {"files": []}  # tier 2 finds nothing (and no candidate)
        if idx == 2:
            assert kwargs.get("pageToken") is None
            # page 1: only the suffix variant
            return {
                "files": [{"id": "FILE_SUFFIX", "name": "GX010067 (1).MP4"}],
                "nextPageToken": "PAGE2",
            }
        if idx == 3:
            assert kwargs.get("pageToken") == "PAGE2"
            # page 2: exact-name file appears -> must win
            return {"files": [{"id": "FILE_EXACT", "name": "GX010067.MP4"}]}
        pytest.fail("unexpected extra call")

    service = _make_drive_service(handler)
    v = _make_video()

    item = v._find_drive_video_file(service, FOLDER_ID, FILE_NAME, LIST_KWARGS)

    assert item["id"] == "FILE_EXACT"
    assert len(service._calls) == 4


def test_tier3_short_circuits_on_exact_in_first_page():
    def handler(idx, q, kwargs):
        if idx == 0:
            return {"files": []}
        if idx == 1:
            return {"files": []}
        if idx == 2:
            # exact name appears in first page; tier 3 should not request page 2
            return {
                "files": [
                    {"id": "FILE_EXACT", "name": "GX010067.MP4"},
                    {"id": "ignored", "name": "GX010067 (1).MP4"},
                ],
                "nextPageToken": "PAGE2",
            }
        pytest.fail("tier 3 must short-circuit; should not request next page")

    service = _make_drive_service(handler)
    v = _make_video()

    item = v._find_drive_video_file(service, FOLDER_ID, FILE_NAME, LIST_KWARGS)

    assert item["id"] == "FILE_EXACT"
    assert len(service._calls) == 3


# --------------------------------------------------------------------------- #
# Negative path                                                                #
# --------------------------------------------------------------------------- #


def test_returns_none_when_no_matches_anywhere():
    def handler(idx, q, kwargs):
        if idx == 0:
            return {"files": []}
        if idx == 1:
            return {"files": [{"id": "noise", "name": "GX010099.MP4"}]}
        if idx == 2:
            return {"files": [{"id": "noise2", "name": "GX010098.MP4"}]}
        pytest.fail("unexpected extra call")

    service = _make_drive_service(handler)
    v = _make_video()

    item = v._find_drive_video_file(service, FOLDER_ID, FILE_NAME, LIST_KWARGS)

    assert item is None


def test_pattern_does_not_match_unrelated_suffixes():
    # 'GX010067_extra.MP4' must NOT match the '<base> (N).<ext>' relaxed pattern
    def handler(idx, q, kwargs):
        if idx == 0:
            return {"files": []}
        if idx == 1:
            return {"files": [{"id": "noise", "name": "GX010067_extra.MP4"}]}
        if idx == 2:
            return {"files": [{"id": "noise", "name": "GX010067_extra.MP4"}]}
        pytest.fail("unexpected extra call")

    service = _make_drive_service(handler)
    v = _make_video()

    item = v._find_drive_video_file(service, FOLDER_ID, FILE_NAME, LIST_KWARGS)

    assert item is None


# --------------------------------------------------------------------------- #
# Sanity checks on query construction                                         #
# --------------------------------------------------------------------------- #


def test_tier1_query_filters_trashed():
    def handler(idx, q, kwargs):
        assert "trashed = false" in q
        return {"files": [{"id": "FILE_A", "name": "GX010067.MP4"}]}

    service = _make_drive_service(handler)
    v = _make_video()
    v._find_drive_video_file(service, FOLDER_ID, FILE_NAME, LIST_KWARGS)


def test_escape_drive_query_value_handles_quotes_and_backslashes():
    assert Video._escape_drive_query_value("ab'cd") == "ab\\'cd"
    assert Video._escape_drive_query_value("a\\b") == "a\\\\b"
    assert Video._escape_drive_query_value("plain.MP4") == "plain.MP4"


def test_clean_id_strips_whitespace_and_handles_none():
    # Trailing whitespace in Airtable cells (e.g. 'GX010067 ') was silently
    # propagating into Drive file-name queries and breaking the lookup.
    assert Video._clean_id("GX010067 ") == "GX010067"
    assert Video._clean_id("  S00430003  ") == "S00430003"
    assert Video._clean_id(None) == ""
    assert Video._clean_id("") == ""
    assert Video._clean_id(123) == "123"


def test_tier3_does_not_mutate_caller_kwargs():
    original_fields = LIST_KWARGS["fields"]

    def handler(idx, q, kwargs):
        if idx in (0, 1):
            return {"files": []}
        # tier 3
        return {"files": []}

    service = _make_drive_service(handler)
    v = _make_video()
    v._find_drive_video_file(service, FOLDER_ID, FILE_NAME, dict(LIST_KWARGS))

    # caller's LIST_KWARGS untouched
    assert LIST_KWARGS["fields"] == original_fields
