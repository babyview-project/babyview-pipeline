import glob
import os
import argparse
from utils import process_imu_for_video_dir


metadata_dir = "/Users/ezhang61/Documents/babyview-pipeline/data/imu_local/metadata_zips/S00380003_S00380003_2026-01-06_1_recIEWCcKVbE4VnDt_metadata"
imu_df = process_imu_for_video_dir(metadata_dir)
if imu_df is None:
    print(f"IMU txt files missing in {metadata_dir}")
else:
    imu_csv_path = os.path.join(metadata_dir, "imu_combined.csv")
    imu_df.to_csv(imu_csv_path, index=False)
    print(f"IMU data saved to {imu_csv_path}")
