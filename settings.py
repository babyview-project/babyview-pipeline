import json
import platform

google_api_token_path = "creds/google_api_token.json"
google_api_credential_path = "creds/credentials.json"
airtable_access_token_path = "creds/airtable_access_token.json"
slack_webhook_path = "creds/slack_webhook.json"
gcp_service_account_path = "creds/hs-babyview-sa.json"

google_drive_entry_point_folder_names = ["BabyView_Main", "BabyView_Bing"]

raw_file_root = "data/bv_tmp/raw/"
process_file_root = "data/bv_tmp/processed/"
error_log = "error_log.txt"

# Prefer the Linux ELF build on the VM; ./gpmf-parser-exec is a macOS arm64 binary.
gpmf_parser_location = (
    '../gpmf-parser/build/gpmf-parser'
    if platform.system() == 'Linux'
    else './gpmf-parser-exec'
)
is_h264_nvenc_available = False

babyview_drive_id = '0AJtfZGZvxvfxUk9PVA'

# Soft-delete Drive raw files this many days after pipeline_run_date (post-run step).
drive_soft_delete_days_old = 180

execute_databrary_uploader = True
databrary_token_url = "https://api.databrary.org/o/token/"
databrary_initiate_upload_url = "https://api.databrary.org/uploads/initiate/"
databrary_sessions_url_template = "https://api.databrary.org/volumes/{volume_id}/sessions"
databrary_credentials_file_path = "creds/databrary_api_secrets.json"
# 🔹 Load credentials dynamically
try:
    with open(databrary_credentials_file_path, "r") as f:
        _creds = json.load(f)
        databrary_user_agent = _creds.get("user_agent")
        databrary_client_id = _creds.get("client_id")
        databrary_client_secret = _creds.get("client_secret")
except Exception as e:
    print(f"[WARN] Could not read Databrary credentials: {e}")
    databrary_user_agent = None
    databrary_client_id = None
    databrary_client_secret = None


# Your two Databrary volumes
databrary_volume_main = 1882   # BV-main / Luna
databrary_volume_bing = 1856   # Bing dataset