"""
cd object-detection/imu
python main.py \
    --imu_metadata_dir /ccn2/dataset/babyview/unzip_2025/babyview_main_storage/00820001_2024-09-02_1_456d4d01b3/ \
    --viz_dir viz/
"""

import os
import argparse
from utils import get_imu_data_lists_for_time_interval, plot_imu_signals

def main(args):
    print(args)

    # Get the IMU data lists for the specified time interval
    imu_data_lists_for_time_interval = get_imu_data_lists_for_time_interval(imu_metadata_dir=args.imu_metadata_dir, time_start_sec=args.time_start_sec, time_end_sec=args.time_end_sec)

    # Plot the IMU signals
    imu_fig, _ = plot_imu_signals(imu_data_lists_for_time_interval, time_start_sec=args.time_start_sec)

    # Save the figure if a visualization directory is specified
    if args.viz_dir is not None:
        if not os.path.exists(args.viz_dir):
            os.makedirs(args.viz_dir)
        imu_fig.savefig(os.path.join(args.viz_dir, 'imu_signals.png'))



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="IMU object detection main script.")
    parser.add_argument('--imu_metadata_dir', type=str, required=True, help='Directory containing IMU metadata')
    parser.add_argument('--viz_dir', type=str)
    parser.add_argument('--time_start_sec', type=int, default=4, help='Start time in seconds for processing IMU data')
    parser.add_argument('--time_end_sec', type=int, default=14, help='End time in seconds for processing IMU data')
    args = parser.parse_args()
    main(args)
    