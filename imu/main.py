"""
cd imu
python /ccn2/u/khaiaw/Code/babyview-pose/babyview-pipeline/imu/main.py \
    --overall_video_dir /ccn2/dataset/babyview/unzip_2025/babyview_main_storage
"""

import glob
import os
import argparse
from utils import process_imu_for_video_dir
import ray

def create_imu_csv(args, accel_txt_path):
    video_dir = os.path.dirname(accel_txt_path)
    video_id = os.path.basename(video_dir)
    output_csv_path = accel_txt_path.replace('ACCL_meta.txt', f'imu_combined_{video_id}.csv')
    try:
        imu_df = process_imu_for_video_dir(video_dir)
        imu_df.to_csv(output_csv_path, index=False)
        print(f"IMU data saved to {output_csv_path}")
    except Exception as e:
        print(f"Error processing {video_dir}: {e}")
        
@ray.remote
def create_imu_csv_remote(args, accel_txt_path):
    return create_imu_csv(args, accel_txt_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="IMU object detection main script.")
    parser.add_argument('--overall_video_dir', type=str, required=True, help='Directory containing video files')
    args = parser.parse_args()
    print(args)
    
    # glob all ACCL_meta.txt files
    accl_txt_files = glob.glob(os.path.join(args.overall_video_dir, '**', 'ACCL_meta.txt'), recursive=True)
    accl_txt_files.sort()
    print(f"Found {len(accl_txt_files)} ACCL_meta.txt files.")
    
    # NOTE: Uncomment below to run sequentially, using a single process
    # for accel_txt_path in accl_txt_files:
    #     print(f"Processing {accel_txt_path}")
    #     create_imu_csv(args, accel_txt_path)

    # NOTE: Uncomment below if you want to run in parallel using Ray
    ray.init()
    futures = [create_imu_csv_remote.remote(args, accel_txt_path) for accel_txt_path in accl_txt_files]
    ray.get(futures)