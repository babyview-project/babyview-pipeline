# databrary_client.py
import os
import json
from datetime import datetime
from typing import List, Dict, Any, Tuple
from dateutil import parser
import requests
import pytz

import settings
from airtable_services import airtable_services
from video import Video

TOKEN_URL = settings.databrary_token_url
INITIATE_UPLOAD_URL = settings.databrary_initiate_upload_url
SESSIONS_URL_TEMPLATE = settings.databrary_sessions_url_template

BV_MAIN_VOLUME = settings.databrary_volume_main
BING_VOLUME = settings.databrary_volume_bing

TOKEN_FILE_PATH = "creds/databrary_tokens.json"
SECRET_FILE_PATH = "creds/databrary_api_secrets.json"

USER_AGENT = settings.databrary_user_agent
CLIENT_ID = settings.databrary_client_id
CLIENT_SECRET = settings.databrary_client_secret


class DatabraryClient:
    """
    Databrary upload helper that:
      - reads/writes token JSON from creds/databrary_tokens.json
      - never raises; instead uses error_log
      - ALWAYS updates Airtable video row with:
          databrary_upload_date
          databrary_upload_status_url
    """

    # ----------------------
    # TOKEN FILE I/O
    # ----------------------
    @staticmethod
    def _load_token_json() -> Tuple[Dict[str, Any] | None, str | None]:
        if not os.path.exists(TOKEN_FILE_PATH):
            return None, f"Token file not found: {TOKEN_FILE_PATH}"

        try:
            with open(TOKEN_FILE_PATH, "r") as f:
                data = json.load(f)
            return data, None
        except Exception as e:
            return None, f"_load_token_json error: {e}"

    @staticmethod
    def _save_token_json(token_json: Dict[str, Any]) -> str | None:
        """
        Overwrite the token file with the new response JSON.
        """
        try:
            os.makedirs(os.path.dirname(TOKEN_FILE_PATH), exist_ok=True)
            with open(TOKEN_FILE_PATH, "w") as f:
                json.dump(token_json, f)
            return None
        except Exception as e:
            return f"_save_token_json error: {e}"

    # ----------------------
    # TOKEN REFRESH
    # ----------------------
    def get_valid_access_token(self) -> Tuple[str | None, str | None]:
        """
        Requirement:
          - each time: read JSON, use refresh_token to get new token,
            save new JSON back, then use the new access_token.

        Returns: (access_token or None, error_message or None)
        """
        stored, err = self._load_token_json()
        if err:
            return None, f"TOKEN_LOAD: {err}"
        if not stored:
            return None, "TOKEN_LOAD: empty token file"

        refresh_token = stored.get("refresh_token")
        if not refresh_token:
            return None, "TOKEN_LOAD: refresh_token missing in token json"

        data = {
            "grant_type": "refresh_token",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": refresh_token,
        }
        headers = {
            "User-Agent": USER_AGENT,
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }

        try:
            resp = requests.post(TOKEN_URL, headers=headers, data=data, timeout=30)
        except Exception as e:
            return None, f"TOKEN_REFRESH: request exception: {e}"

        if resp.status_code != 200:
            return None, f"TOKEN_REFRESH: HTTP {resp.status_code} {resp.text}"

        try:
            token_json = resp.json()
        except Exception as e:
            return None, f"TOKEN_REFRESH: parse json error: {e}"

        save_err = self._save_token_json(token_json)
        if save_err:
            # we still can use the token, but log the save error
            return token_json.get("access_token"), f"TOKEN_REFRESH_SAVE: {save_err}"

        access_token = token_json.get("access_token")
        if not access_token:
            return None, "TOKEN_REFRESH: no access_token in response"

        return access_token, None

    # ----------------------
    # VOLUME / SESSIONS
    # ----------------------
    def _get_volume_id_from_dataset(self, dataset: str) -> int:
        """
        dataset from Airtable: 'BV-main'/'Luna' or 'Bing'.
        """
        if not dataset:
            return BV_MAIN_VOLUME
        ds = dataset.lower()
        if "bing" in ds:
            return BING_VOLUME
        return BV_MAIN_VOLUME

    def _fetch_all_sessions(self, volume_id: int, access_token: str) -> Tuple[List[Dict[str, Any]] | None, str | None]:
        """
        POST /volumes/{volume_id}/sessions (paginated)
        """
        headers = {
            "User-Agent": USER_AGENT,
            "Content-Type": "application/octet-stream",
            "Authorization": f"Bearer {access_token}",
        }
        url = SESSIONS_URL_TEMPLATE.format(volume_id=volume_id)
        all_results: List[Dict[str, Any]] = []

        while url:
            try:
                resp = requests.post(url, headers=headers, data=b"", timeout=30)
            except Exception as e:
                return None, f"SESSIONS: request exception: {e}"

            if resp.status_code != 200:
                return None, f"SESSIONS: HTTP {resp.status_code} {resp.text}"

            try:
                data = resp.json()
            except Exception as e:
                return None, f"SESSIONS: parse json error: {e}"

            results = data.get("results", [])
            all_results.extend(results)
            url = data.get("next")

        return all_results, None

    def _find_object_id_for_subject(
            self,
            sessions: List[Dict[str, Any]],
            subject_id: str,
            volume_id: int,
    ) -> Tuple[int | None, str | None]:
        """
        Match Databrary session where name == subject_id and volume == volume_id.
        """
        if not subject_id:
            return None, "SESSION_MATCH: subject_id is empty"

        for s in sessions:
            try:
                if (
                        s.get("name", "").strip().lower() == subject_id.strip().lower()
                        and int(s.get("volume")) == int(volume_id)
                ):
                    return s.get("id"), None
            except Exception as e:
                # continue but log last error
                last_err = f"SESSION_MATCH: iteration error: {e}"
        # if we get here, not found
        return None, f"SESSION_MATCH: no session found for subject_id={subject_id}, volume={volume_id}"

    def _list_files_for_session(
            self,
            access_token: str,
            volume_id: int,
            session_id: int,
    ) -> Tuple[List[Dict[str, Any]] | None, str | None]:
        base_url = f"https://api.databrary.org/volumes/{volume_id}/sessions/{session_id}/files/"
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
        }

        all_results: List[Dict[str, Any]] = []

        page = 1
        total_pages: int | None = None

        while True:
            # First page: use base_url; subsequent pages: add ?page=N
            if page == 1:
                url = base_url
            else:
                url = f"{base_url}?page={page}"

            try:
                resp = requests.get(url, headers=headers, timeout=30)
            except Exception as e:
                return None, f"FILES: request exception on page {page}: {e}"

            if resp.status_code != 200:
                return None, f"FILES: HTTP {resp.status_code} on page {page}: {resp.text}"

            try:
                data = resp.json()
            except Exception as e:
                return None, f"FILES: parse json error on page {page}: {e}"

            results = data.get("results", [])
            all_results.extend(results)

            # Use totalPages from the payload; default to 1 if missing
            if total_pages is None:
                total_pages = data.get("totalPages") or 1

            # Stop if we've reached the last page
            if page >= total_pages:
                break

            page += 1

        return all_results, None

    def _find_file_id_for_video(
            self,
            files: List[Dict[str, Any]],
            video: Video,
            filename: str,
    ) -> Tuple[int | None, str | None]:
        """
        Try to find the Databrary file that corresponds to this upload.

        Strategy:
          1) Prefer exact match on upload.filename == <filename we just uploaded>.
          2) Fallback: if video.unique_video_id is present, check if it is contained
             in the Databrary file 'name' field.
        """
        if not files:
            return None, "FILES_MATCH: empty file list"

        # 1) exact match on upload.filename
        for f in files:
            try:
                upload_info = f.get("upload") or {}
                fname = upload_info.get("filename") or ""
                if fname == filename:
                    return f.get("id"), None
            except Exception as e:
                # just skip and continue
                continue

        # 2) fallback: unique_video_id substring in name
        uid = getattr(video, "unique_video_id", None)
        if uid:
            for f in files:
                name = (f.get("name") or "")
                if uid in name:
                    return f.get("id"), None

        return None, f"FILES_MATCH: no file found for filename={filename}"

    def _get_source_date_for_video(self, video: Video) -> Tuple[str | None, str | None]:
        """
        Convert video.date into 'YYYY-MM-DD' string for Databrary PATCH payload.

        Assumes video.date is either:
          - a string like '2024-03-15' or '2024-03-15T10:20:30Z'
          - a datetime
        """
        raw = getattr(video, "date", None)
        if raw is None:
            return None, "SOURCE_DATE: video.date is None"

        # datetime -> YYYY-MM-DD
        if isinstance(raw, datetime):
            return raw.strftime("%Y-%m-%d"), None

        # string -> try first 10 chars if looks like YYYY-MM-DD
        if isinstance(raw, str):
            s = raw.strip()
            if not s:
                return None, "SOURCE_DATE: video.date is empty string"
            try:
                s = parser.parse(raw).strftime('%Y-%m-%d')
                return s, None
            except Exception as e:
                return None, f"SOURCE_DATE: can't convert {type(raw)} for video.date, {e}"

        return None, f"SOURCE_DATE: unsupported type {type(raw)} for video.date"

    def _patch_file_source_date(
            self,
            access_token: str,
            volume_id: int,
            session_id: int,
            file_id: int,
            source_date: str,
    ) -> str | None:
        """
        PATCH /volumes/{volume_id}/sessions/{session_id}/files/{file_id}/
        body: {"source_date": "YYYY-MM-DD"}
        """
        url = (
            f"https://api.databrary.org/volumes/{volume_id}/sessions/"
            f"{session_id}/files/{file_id}/"
        )
        headers = {
            "User-Agent": USER_AGENT,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
        }
        payload = {"source_date": source_date}

        try:
            resp = requests.patch(url, headers=headers, json=payload, timeout=30)
        except Exception as e:
            return f"FILES_PATCH: request exception: {e}"

        if not (200 <= resp.status_code < 300):
            return f"FILES_PATCH: HTTP {resp.status_code} {resp.text}"

        return None

    # ----------------------
    # INITIATE + UPLOAD
    # ----------------------
    def _initiate_upload(self, access_token: str, filename: str, object_id: int) -> Tuple[
        Dict[str, Any] | None, str | None]:
        headers = {
            "User-Agent": USER_AGENT,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
        }
        payload = {
            "filename": filename,
            "destination_type": "session",
            "object_id": object_id,
        }
        try:
            resp = requests.post(INITIATE_UPLOAD_URL, headers=headers, json=payload, timeout=30)
        except Exception as e:
            return None, f"INITIATE: request exception: {e}"

        if resp.status_code not in (200, 201):
            return None, f"INITIATE: HTTP {resp.status_code} {resp.text}"

        try:
            data = resp.json()
        except Exception as e:
            return None, f"INITIATE: parse json error: {e}"

        return data, None

    def _upload_file_to_signed_url(self, access_token: str, signed_url: str, local_path: str) -> str | None:
        """
        PUT binary to signedUploadUrl
        """
        headers = {
            "User-Agent": USER_AGENT,
            "Content-Type": "application/octet-stream",
            "Authorization": f"Bearer {access_token}",
        }
        if not os.path.exists(local_path):
            return f"UPLOAD: local file not found: {local_path}"

        try:
            with open(local_path, "rb") as f:
                resp = requests.put(signed_url, headers=headers, data=f, timeout=600)
        except Exception as e:
            return f"UPLOAD: request exception: {e}"

        if not (200 <= resp.status_code < 300):
            return f"UPLOAD: HTTP {resp.status_code} {resp.text}"

        return None

    # ----------------------
    # HIGH-LEVEL ENTRY
    # ----------------------
    def upload_video(self, video: Video) -> Tuple[str | None, List[str]]:
        """
        Full Databrary upload workflow for one Video.

        Returns:
            (status_url_or_None, error_log_list)

        Behavior:
            - NEVER raises.
            - ALWAYS writes to Airtable video row:
                databrary_upload_date
                databrary_upload_status_url
              where status_url field is either:
                - real Databrary statusUrl (success), or
                - 'ERROR: ...' message describing where it stopped.
        """
        error_log: List[str] = []
        status_url: str | None = None

        # 1. get access token
        access_token, err = self.get_valid_access_token()
        if err:
            print(err)
            error_log.append(err)
        if not access_token:
            # can't do anything else
            self._update_airtable_status(video, status_url, error_log)
            return status_url, error_log

        # 2. volume + sessions
        volume_id = self._get_volume_id_from_dataset(video.dataset)
        sessions, err = self._fetch_all_sessions(volume_id, access_token)
        if err:
            print(err)
            error_log.append(err)
        if sessions is None:
            self._update_airtable_status(video, status_url, error_log)
            return status_url, error_log

        # 3. match session object_id
        object_id, err = self._find_object_id_for_subject(sessions, video.subject_id, volume_id)
        if err:
            print(err)
            error_log.append(err)
        if object_id is None:
            self._update_airtable_status(video, status_url, error_log)
            return status_url, error_log

        # 4. initiate upload
        filename = os.path.basename(video.compress_video_path)
        print(
            f"Sending {video.unique_video_id} to databrary: v_id_{volume_id}, obj_id_{object_id}, filename: {filename}")
        init_resp, err = self._initiate_upload(access_token, filename, object_id)
        if err:
            print(err)
            error_log.append(err)
        if init_resp is None:
            self._update_airtable_status(video, status_url, error_log)
            return status_url, error_log

        signed_url = init_resp.get("signedUploadUrl")
        status_url = init_resp.get("statusUrl")
        if not signed_url or not status_url:
            msg = f"INITIATE: missing signedUploadUrl or statusUrl in response: {init_resp}"
            print(msg)
            error_log.append(msg)
            self._update_airtable_status(video, status_url, error_log)
            return status_url, error_log
        print(f"signed_url: {signed_url}, status_url: {status_url}")

        # 5. PUT file to signed URL
        upload_err = self._upload_file_to_signed_url(
            access_token=access_token,
            signed_url=signed_url,
            local_path=video.compress_video_path,
        )
        if upload_err:
            error_log.append(upload_err)
            # we at least have the upload status URL
            status_url = status_url
            self._update_airtable_status(video, status_url, error_log)
            return status_url, error_log

        # 6) list files for this session to find file_id
        # files, files_err = self._list_files_for_session(
        #     access_token=access_token,
        #     volume_id=volume_id,
        #     session_id=object_id,
        # )
        # file_id = None
        # if files_err:
        #     error_log.append(files_err)
        #     self._update_airtable_status(video, status_url, error_log)
        #     return status_url, error_log
        # else:
        #     input(f"{files}, {len(files)}")
        #     file_id, match_file_err = self._find_file_id_for_video(files, video, filename)
        #     if match_file_err:
        #         error_log.append(match_file_err)
        #         self._update_airtable_status(video, status_url, error_log)
        #         return status_url, error_log
        #
        # # 7) patch source_date if we found a file_id and video.date is usable
        # if file_id is not None:
        #     source_date, sd_err = self._get_source_date_for_video(video)
        #     if sd_err:
        #         error_log.append(sd_err)
        #         self._update_airtable_status(video, status_url, error_log)
        #         return status_url, error_log
        #     elif source_date:
        #         patch_err = self._patch_file_source_date(
        #             access_token=access_token,
        #             volume_id=volume_id,
        #             session_id=object_id,
        #             file_id=file_id,
        #             source_date=source_date,
        #         )
        #         if patch_err:
        #             error_log.append(patch_err)
        #             self._update_airtable_status(video, status_url, error_log)
        #             return status_url, error_log
        #
        # # 8) final status URL: prefer the file URL if we got file_id
        # if file_id is not None:
        #     status_url = (
        #         f"https://api.databrary.org/volumes/{volume_id}/sessions/"
        #         f"{object_id}/files/{file_id}/"
        #     )

        # 9. final update: regardless of error, write to Airtable
        self._update_airtable_status(video, status_url, error_log)
        return status_url, error_log

    # ----------------------
    # Airtable update helper
    # ----------------------
    @staticmethod
    def _update_airtable_status(video: Video, status_url: str | None, error_log: List[str]) -> None:
        """
        Decide what to store in databrary_upload_status_url and write to Airtable.
        """
        if error_log:
            status_value = "ERROR: " + " | ".join(error_log)
        else:
            if status_url:
                status_value = status_url
            else:
                status_value = "UNKNOWN: no error_log, no status_url"

        try:
            tz = pytz.timezone("America/Los_Angeles")
            date_str = datetime.now(tz).strftime("%Y-%m-%d")

            airtable_services.update_video_table_single_video(
                video.unique_video_id,
                {
                    "databrary_upload_date": date_str,
                    "databrary_upload_status_url": status_value,
                },
            )
        except Exception as e:
            print(f"AIRTABLE_UPDATE: failed for {video.unique_video_id}: {e}")
