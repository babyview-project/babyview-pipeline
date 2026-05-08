# Download videos given user credentials (email & password)
Usage: `python download_videos.py --emaili {EMAIL} --password {PASSWORD} --video_root {VID_ROOT}`

Videos will be saved to *VID_ROOT/FILENAME.MP4*, which matches the video name we see on GoPro Media Library. This is only part of the pipeline, will need to add extract metadata and compress video to the pipeline.

## Issue To Be Resolved
- [ ] Some videos on GoPro Cloud Library has an option to *Request Full-Length Video*. Current method to retrieve download links result in invalid url error.
