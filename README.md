# BabyView Data Management Pipeline

A pipeline for downloading, compressing, and managing video metadata from the GoPro cloud, with automatic upload to GCP storage.

---

## Installation

```sh
pip install -r requirements.txt
```
## Build parser file 
```sh
git clone https://github.com/gopro/gpmf-parser.git
mkdir build
cd build
cmake ..
make
```
Then edit the parser file in settings
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
Edit settings.py to enforce a custom filter, Or
```sh
python main.py --filter_key status --filter_value TEST # process vids with given status 
python main.py # process vids with no pipeline_run_date
```
