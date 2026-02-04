'''
Script to process every recursively-globbed *.MP4 file in specified directory: setting each video to the desired fps, resolution, rotation.
The resulting file is saved as *_processed.MP4. The original file is removed only after the processed file is written.

pip install ffmpeg-python

python scripts/process_videos_same_res.py \
    --videos_dir "/ccn2/dataset/babyview/unzip_2025/babyview_bing_storage/" \
'''

import os
import argparse
import ray
import glob
import ffmpeg
import tqdm
import multiprocessing

def get_args():
    parser = argparse.ArgumentParser(description='Process videos to the desired fps, resolution, rotation.')
    parser.add_argument('--videos_dir', type=str, required=True, help='Path to the directory with videos')
    parser.add_argument('--num_processes', type=int, default=16, help='Number of parallel processes')
    return parser.parse_args()

def get_video_info(file_path):
    probe = ffmpeg.probe(file_path)
    video_streams = [stream for stream in probe['streams'] if stream['codec_type'] == 'video']
    if video_streams:
        frame_rate = video_streams[0]['r_frame_rate']
        width = int(video_streams[0]['width'])
        height = int(video_streams[0]['height'])
        return frame_rate, width, height
    else:
        raise ValueError("No video stream found in the file.")

def process_video(video_file):
    # new_width = 360
    # new_height = 640
    new_frame_rate = '30/1'
    output_file = video_file.replace('.MP4', '_processed.MP4')
    
    if '_processed' in video_file:
        return
    
    # LUNA videos are horizontal, and already at the desired resolution and frame rate
    # frame_rate: 30/1 input_width: 1920 input_height: 1080
    if 'LUNA' in video_file:
        return
    
    do_rotate = False # Track if we need to rotate
    try:
        frame_rate, input_width, input_height = get_video_info(video_file)
        # print('frame_rate:', frame_rate, 'input_width:', input_width, 'input_height:', input_height, video_file)
        
        # If videos are correctly rotated and at the right fps, no need to do processing
        if input_width < input_height:
            if frame_rate in ['30/1', '30000/1001']:
                if os.path.exists(output_file):
                    os.remove(output_file)
                os.rename(video_file, output_file)
                return

        # Get ready to do processing
        output_video = ffmpeg.input(video_file)
        
        if frame_rate != '30/1' and frame_rate != '30000/1001':
            output_video = output_video.filter('fps', new_frame_rate)
        
        if input_width > input_height:
            do_rotate = True
            output_video = output_video.filter('transpose', 2) # Rotate counterclockwise 90 degrees
        
        # Write to disk
        output_audio = ffmpeg.input(video_file).audio
        ffmpeg.output(output_video, output_audio, output_file).run(overwrite_output=True, quiet=True)
        
        # Remove the original file ONLY AT THE END
        # We write out the processed video first so we know it was completely processed. 
        # If this function is interrupted, we want to still have the original
        os.remove(video_file)
        
        # Just for me to check that things are working okay, especially if we rotations
        if do_rotate and os.path.getsize(output_file) > 1e7 and os.path.getsize(output_file) < 5e7:
            print('=== \n \n Rotated and good to check ===')
        print(f"Processed video: {output_file}")
        
    except Exception as e:
        print(f"Error processing video: {video_file}")
        print(e)
    

@ray.remote
def process_video_remote(video_file):
    process_video(video_file)

def main(args):
    video_files = glob.glob(os.path.join(args.videos_dir, '**/*.MP4'), recursive=True)
    # video_files = [v for v in video_files if 'LUNA' in v]
    # video_files = ['/ccn2/dataset/babyview/unzip_2025/babyview_main_storage/00220001_2024-10-09_4_0c1a8fc006/00220001_GX040057_10.07.2024-10.13.2024_10.09.2024-5:56pm.MP4']
    # video_files = [v for v in video_files if '_processed' not in v] # Skip already processed videos
    
    print(len(video_files))
    print('_processed:', len([v for v in video_files if '_processed' in v]))
    print('LUNA:', len([v for v in video_files if 'LUNA' in v]))

    # for video_file in tqdm.tqdm(video_files):
    #     process_video(video_file)

    ray.init(num_cpus=args.num_processes)
    futures = [process_video_remote.remote(video_file) for video_file in video_files]
    ray.get(futures)
    

if __name__ == '__main__':
    args = get_args()
    print(args)
    main(args)
    
