import os
import argparse
import pandas as pd
from tqdm import tqdm

from moviepy.editor import VideoFileClip
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow


ALL_METAS = [
    'ACCL', 'GYRO', 'SHUT', 'WBAL', 'WRGB',
    'ISOE', 'UNIF', 'FACE', 'CORI', 'MSKP',
    'IORI', 'GRAV', 'WNDM', 'MWET', 'AALP',
    'LSKP']



class VideoDuration:
    """ Download processed zip files, and use information inside the meta data to get the duration of the video. """
    def __init__(self, args):
        self.args = args
        self.storage_drive_id = '0AJGltX6vgytGUk9PVA'
        self.SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive']
        self.total_video_count = 0        
        self.video_durations = []
        self.load_duplicate_file_paths()


    def load_duplicate_file_paths(self):
        # load the duplicate file paths
        duplicate_file_path = 'new_duplicate_txt_files.csv'
        duplicate_df = pd.read_csv(duplicate_file_path)
        self.duplicate_file_names = [f.replace('.txt', '.MP4') for f in duplicate_df['File2'].values]    
    

    def check_duration(self):
        for subject_id in os.listdir(self.args.path):
            print(f"Checking subject: {subject_id}")            
            subject_path = os.path.join(self.args.path, subject_id)
            for video_file in tqdm(os.listdir(subject_path)):
                if video_file not in self.duplicate_file_names:                    
                    video_path = os.path.join(subject_path, video_file)
                    video = VideoFileClip(video_path)
                    duration = video.duration
                    video.close()
                    size_bytes = os.path.getsize(video_path)
                    size_mb = size_bytes / (1024 * 1024)
                    self.video_durations.append([video_path, duration, size_mb])
                    self.total_video_count += 1

        # save the video durations
        df = pd.DataFrame(self.video_durations, columns=['File Path', 'Duration', 'Size'])
        df.to_csv(self.args.output, index=False)        



def main():
    parser = argparse.ArgumentParser(description="Data management pipeline for GoPro videos")
    # downloader args    
    parser = argparse.ArgumentParser(description="Download videos from cloud services")
    parser.add_argument('--path', type=str, default='/data/yinzi/babyview_20240503/', help='root to saved videos')
    parser.add_argument('--output', type=str, default='video_durations_local.csv', help='output file path')
    args = parser.parse_args()
    downloader = VideoDuration(args)
    downloader.check_duration()
    
    
if __name__ == '__main__':
    main()
