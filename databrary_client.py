# databrary_client.py
import os
import json
from datetime import datetime
from typing import List, Dict, Any, Tuple

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
            "source_date": "2024-03-15"
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

        signed = init_resp.get("signedUploadUrl")
        status_url = init_resp.get("statusUrl")
        if not signed or not status_url:
            msg = f"INITIATE: missing signedUploadUrl or statusUrl in response: {init_resp}"
            print(msg)
            error_log.append(msg)
            self._update_airtable_status(video, status_url, error_log)
            return status_url, error_log
        print(f"signed_url: {signed}, status_url: {status_url}")

        # 5. PUT file to signed URL
        err = self._upload_file_to_signed_url(access_token, signed, video.compress_video_path)
        if err:
            print(err)
            error_log.append(err)

        # 6. final update: regardless of error, write to Airtable
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
