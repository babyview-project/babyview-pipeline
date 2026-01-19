"""
cd object-detection/imu
python create_imu_csv_for_10s_clips.py \
    --overall_video_dir /ccn2/dataset/babyview/unzip_2025/ \
    --out_dir /ccn2/dataset/babyview/outputs_20250312/imu/10s_clips/
"""

import glob
import os
import argparse
from utils import process_imu_for_video_dir, get_imu_data_lists_for_time_interval, plot_imu_signals
import ray

def create_imu_csv_for_10s_clips(args, video_path):

    # Process IMU for video dir
    video_dir = os.path.dirname(video_path)
    imu_df = process_imu_for_video_dir(video_dir)
    if imu_df is None:
        return
    basename = os.path.basename(video_path).split('.')[0]

    # imu_df: (['Timestamp (s)', 'ACCL_X (m/s²)', 'ACCL_Y (m/s²)', 'ACCL_Z (m/s²)', 'GYRO_X (rad/s)', 'GYRO_Y (rad/s)', 'GYRO_Z (rad/s)', 'GRAV_X (m/s²)', 'GRAV_Y (m/s²)', 'GRAV_Z (m/s²)'])
    # groupby 10s chunks of Timestamp, then get an average value for each of the other columns
    imu_df['Timestamp (s)'] = imu_df['Timestamp (s)'].astype(float)
    cols_to_avg = imu_df.columns.difference(['Timestamp (s)'])
    out = (
        imu_df
        .assign(ChunkNum=(imu_df['Timestamp (s)'] // 10).astype(int))  # 0–9.999 -> 0, 10–19.999 -> 1, etc.
        .groupby('ChunkNum', as_index=False)[cols_to_avg]
        .agg(lambda x: x.abs().mean())  # averages mean absolute value; use (x**2) for squared values if preferred
    )

    out['video_id'] = out['ChunkNum'].apply(lambda x, b=basename: f"{b}_{x:03d}.mp4")
    out_path = os.path.join(args.out_dir, f"{basename}.csv")
    os.makedirs(args.out_dir, exist_ok=True)
    out.to_csv(out_path, index=False)

@ray.remote
def process_video(args, video_path):
    try:
        create_imu_csv_for_10s_clips(args, video_path)
    except Exception as e:
        print(f"Error processing {video_path}: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="IMU object detection main script.")
    parser.add_argument('--overall_video_dir', type=str, required=True, help='Directory containing video files')
    parser.add_argument('--out_dir', type=str, required=True, help='Output directory for IMU CSV files')
    args = parser.parse_args()
    print(args)
    
    # glob all mp4 files
    video_files = glob.glob(os.path.join(args.overall_video_dir, '**', '*.[Mm][Pp]4'), recursive=True)
    video_files.sort()

    ray.init()
    futures = [process_video.remote(args, video_path) for video_path in video_files]
    ray.get(futures)