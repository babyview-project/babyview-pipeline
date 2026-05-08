import os
import dateutil
import pandas as pd


def convert_seconds(seconds):
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"


file_path = 'video_durations_local.csv'
df = pd.read_csv(file_path)
# iterate through rows
full_paths = df['File Path'].values
full_paths_dates_durations = []


# load the duplicate file paths
duplicate_file_path = 'new_duplicate_txt_files.csv'
duplicate_df = pd.read_csv(duplicate_file_path)
duplicate_file_names = [f.replace('.txt', '.MP4') for f in duplicate_df['File2'].values]


for i, full_path in enumerate(full_paths):        
    duration = df['Duration'][df['File Path'] == full_path].values[0]
    file_path = os.path.basename(full_path)

    # only process if the file path is not in the duplicate file names
    if file_path not in duplicate_file_names:    
        if len(file_path.split('_')) == 4:
            subject_id, video_id, week, datetime = file_path.split('_')
        elif len(file_path.split('_')) == 5:
            subject_id, video_id, num, week, datetime = file_path.split('_')
        else:
            # these paths need to be ignored because they are not in the correct format
            print("Odd file paths:", file_path)
            subject_id, video_id, _, _, _, _, _, datetime = file_path.split('_')        
            week = None        

                
        if week or datetime:        
            # get the date from datetime
            try:
                # first try to get date from datetime
                if len(datetime.split('-')) == 2:                
                    date = datetime.split('-')[0]
                else:
                    date = '.'.join(datetime.split('-')[:3])
                date = dateutil.parser.parse(date)
            except:            
                # usually this fails due to NA values or None, in this case we use first day of the week to get date
                first_day_of_week = week.split('-')[0]
                date = dateutil.parser.parse(first_day_of_week)

            full_paths_dates_durations.append({"full_paths": full_path, "date": date, "duration": duration})

output_df = pd.DataFrame(full_paths_dates_durations)
output_df.to_csv('video_durations_with_dates.csv', index=False)
total_duration = output_df['duration'].sum()
total_duration = convert_seconds(total_duration)
print(f"Total duration: {total_duration}")
# compute total duration (LUNA)
df_one = output_df[output_df['full_paths'].str.contains('00270001_H', case=False, na=False)]
one_total_duration = df_one['duration'].sum()
one_total_duration = convert_seconds(one_total_duration)
print(f"Total LUNA duration: {one_total_duration}")
# compute total duration (MAIN)
df_main = output_df[~output_df['full_paths'].str.contains('00270001_H', case=False, na=False)]
main_total_duration = df_main['duration'].sum()
main_total_duration = convert_seconds(main_total_duration)
print(f"Total MAIN duration: {main_total_duration}")