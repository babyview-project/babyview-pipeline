# BabyView Data Management Pipeline
## Installation
```
pip install -r requirements.txt
```
## To count number of .MP4 videos on drive
```
python count_videos.py
```

## Usage
To use the full pipeline, you must have account information for GoPro cloud as well as AWS *access_key* and *secret_key*. 
To get videos downloaded, compressed, metadata extracted and videos uploaded to AWS bucket.
`python main.py --video_root [store_downloaded_videos] --output_folder [store_compressed_videos_and_metadata] --email [email] --password [password] --aws_access_key [aws_access_key] --aws_secret_key [aws_secret_key]`
