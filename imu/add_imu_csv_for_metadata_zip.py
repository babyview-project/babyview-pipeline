"""
python /ccn2/u/khaiaw/Code/babyview-pose/babyview-pipeline/imu/add_imu_csv_for_metadata_zip.py \
    --overall_video_dir /ccn2/dataset/babyview/imu_processing/
"""

import glob
import os
import argparse
import zipfile
import shutil
from utils import process_imu_for_video_dir
import ray


def zip_has_accl(zip_path: str) -> bool:
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            if name.endswith("ACCL_meta.txt") and os.path.basename(name) == "ACCL_meta.txt":
                return True
    return False


def zip_has_imu(zip_path: str) -> bool:
    # Skip if ANY file in the zip has basename starting with "imu_"
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            base = os.path.basename(name.rstrip("/"))
            if base.startswith("imu_"):
                return True
    return False


def rezip_dir(unzip_dir: str, out_zip_path: str) -> None:
    tmp_zip_path = out_zip_path + ".tmp"
    with zipfile.ZipFile(tmp_zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for root, _, files in os.walk(unzip_dir):
            for fn in files:
                abs_path = os.path.join(root, fn)
                rel_path = os.path.relpath(abs_path, unzip_dir)
                zf.write(abs_path, arcname=rel_path)
    os.replace(tmp_zip_path, out_zip_path)


def create_imu_csv(args, zip_path):
    # unzip directory should be same name as the zipfile
    unzip_dir = os.path.splitext(zip_path)[0]  # "/a/b/foo.zip" -> "/a/b/foo"

    try:
        # Check zip contents first
        if not zip_has_accl(zip_path):
            print(f"Skipping (no ACCL_meta.txt): {zip_path}")
            return
        if zip_has_imu(zip_path):
            print(f"Skipping (already has imu_ file): {zip_path}")
            return

        # Fresh unzip dir
        if os.path.exists(unzip_dir):
            shutil.rmtree(unzip_dir)
        os.makedirs(unzip_dir, exist_ok=True)

        # Unzip
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(unzip_dir)

        # Find ACCL_meta.txt inside extracted tree
        accl_txt_files = glob.glob(os.path.join(unzip_dir, "**", "ACCL_meta.txt"), recursive=True)
        accl_txt_files.sort()
        if len(accl_txt_files) == 0:
            print(f"Skipping (ACCL_meta.txt not found after unzip): {zip_path}")
            return

        accl_txt_path = accl_txt_files[0]
        video_dir = os.path.dirname(accl_txt_path)
        video_id = os.path.basename(video_dir) or os.path.basename(unzip_dir)

        output_csv_path = accl_txt_path.replace("ACCL_meta.txt", f"imu_combined.csv")

        imu_df = process_imu_for_video_dir(video_dir)
        imu_df.to_csv(output_csv_path, index=False)
        print(f"IMU data saved to {output_csv_path}")

        # Zip it back up to replace the original one
        rezip_dir(unzip_dir, zip_path)

    except Exception as e:
        print(f"Error processing {zip_path}: {e}")
        # If something went wrong, keep the unzip_dir for debugging (don’t auto-delete here)
    finally:
        # Optional: clean up extracted dir to save space
        shutil.rmtree(unzip_dir, ignore_errors=True)


@ray.remote
def create_imu_csv_remote(args, zip_path):
    return create_imu_csv(args, zip_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IMU zip processing main script.")
    parser.add_argument("--overall_video_dir", type=str, required=True, help="Directory containing zip files")
    args = parser.parse_args()
    print(args)

    # glob all .zip files
    zip_files = glob.glob(os.path.join(args.overall_video_dir, "**", "*.zip"), recursive=True)
    zip_files.sort()
    print(f"Found {len(zip_files)} .zip files.")
    
    # Debugging
    # zip_files = ['/data/datasets/babyview/imu_processing/babyview_main_storage/S00320003/S00320003_2025-11-11_3_recCV6OaLv9MkRFT7_metadata.zip']
    
    # Count how many files have imu_combined.csv already
    count_with_imu = 0
    for zip_path in zip_files:
        if zip_has_imu(zip_path):
            count_with_imu += 1
    print(f"{count_with_imu} / {len(zip_files)} zip files already have imu_combined.csv.")
    
    # save all the files that do not have imu_combined.csv into a txt file
    zip_files = [zip_path for zip_path in zip_files if not zip_has_imu(zip_path)]
    print(f"{len(zip_files)} zip files to process.")
    with open("zip_files_with_issues.txt", "w") as f:
        for zip_path in zip_files:
            f.write(zip_path + "\n")
    
    # Debugging
    # zip_files = ['/data/datasets/babyview/imu_processing/babyview_main_storage/S00220001/S00220001_2024-02-05_1_recCJHK2DuH51YqtH_metadata.zip']

    # NOTE: Uncomment below to run sequentially, using a single process
    # for zip_path in zip_files:
    #     print(f"Processing {zip_path}")
    #     create_imu_csv(args, zip_path)

    # NOTE: Uncomment below if you want to run in parallel using Ray
    ray.init()
    futures = [create_imu_csv_remote.remote(args, zip_path) for zip_path in zip_files]
    ray.get(futures)
