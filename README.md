# BabyView Data Management Pipeline

A pipeline for downloading, compressing, and managing video metadata from the GoPro cloud, with automatic upload to GCP storage.

---

## Installation

```sh
pip install -r requirements.txt
```

## Creds and temp file path setup

1. Prepare to have credentials.json, hs-babyview-sa.json and token.json ready in the creds folder
2. Setup cred_folder in settings.py.
3. Setup video_root and output_folder in settings.py.
4. Setup is_h264_nvenc_available depends on nvenc is available on the hosting machine.

## Features

### Count MP4 Videos
#### Quickly count the number of .MP4 files on your drive:

```sh
python count_videos.py
```

### Run main function 
```sh
python main.py \
--bv_type 'main/bing/luna' \  # choose from main/bing/luna
--subject_id 'all/00240001' \ # choose from all or individual subject_id
--tracking_sheet_idx_start_stop '2390 2390' # optional select a range of idx to be process
```