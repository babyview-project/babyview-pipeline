google_api_token_path = "creds/google_api_token.json"
google_api_credential_path = "creds/credentials.json"
airtable_access_token_path = "creds/airtable_access_token.json"
gcp_service_account_path = "creds/hs-babyview-sa.json"

google_drive_entry_point_folder_names = ["BabyView_Main", "BabyView_Bing"]

raw_file_root = "data/bv_tmp/raw/"
process_file_root = "data/bv_tmp/processed/"
error_log = "error_log.txt"

gpmf_parser_location = './gpmf-parser-exec'
is_h264_nvenc_available = False

babyview_drive_id = '0AJtfZGZvxvfxUk9PVA'

forced_filter = False
forced_filter_key = 'unique_video_id'
forced_filter_value = [
    # 'rec00KRZq9bT8l8nc', #bv_main reg
    # 'rec03VOaeG6dftcIg',  # luna reg
    # 'rec0NwEqXa9gYtbyX',  # bing reg
    # 'recsc7rperGsfmWSw',  # bv_main with blackout
    # 'recbuxCVAkXCuUWK7',  # bv_main reg
    # 'recGfqdmALp9jP1yE',  # bv_main reg
    'reccEvuBCfsoTiJ12',
]
trash_old_drive_files = []
