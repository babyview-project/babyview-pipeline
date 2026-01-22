## Introduction
BabyView Data includes inertial measurement unit (IMU) information. This data is extracted from the head-mounted camera (GoPro) into text files. But text files are a format that is not very easy to use. 

So we wrote some scripts to:
- For each video, convert the multiple IMU text files (e.g., `ACCL_meta.txt`) into a CSV, with the IMU data in columns, together with the timestamp. (`main.py`, which uses functions written in `utils.py`). Run `main.py` to generate an IMU CSV file for each video in the dataset.
- Visualize IMU data for a user-specified video and time interval (`viz.py`, which uses `utils.py`)

