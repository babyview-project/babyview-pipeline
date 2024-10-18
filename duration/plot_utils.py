import pandas as pd
import matplotlib.pyplot as plt

def extract_participant_id(file_path):
    # Splits the file path and returns the participant ID    
    if 'yinzi' in file_path:
        return file_path.split('/')[-2]
    else:
        return file_path.split('/')[2]    


def seconds_to_hms(seconds):
    # Convert seconds to hours, minutes, and seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"


def hms_to_seconds(hms):
    h, m, s = map(int, hms.split(':'))
    return h * 3600 + m * 60 + s


def calculate_total_duration(data): 
    data['Participant ID'] = data['File Path'].apply(extract_participant_id)
    if 'File Size (MB)' not in data.columns:
        return data.groupby('Participant ID')[['Duration']].sum()
    return data.groupby('Participant ID')[['Duration', 'File Size (MB)']].sum()
    

def plot_total_duration_by_participant(total_durations, title):        
    total_durations.plot(kind='bar', figsize=(10, 5), color='skyblue')
    plt.xlabel('Participant ID')
    plt.ylabel('Total Video Duration (seconds)')
    plt.title(title)
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig(f'{title}.png', dpi=300)


def plot_result(result_path):
    video_data = pd.read_csv(result_path)    
    df = calculate_total_duration(video_data)    
    # plot for each participant
    title = result_path.split('/')[-1].split('.')[0]
    total_durations = df['Duration']
    plot_total_duration_by_participant(total_durations, title)    
    # output csv file in human readable format 
    duration_by_particpants_output = f"{result_path.split('.')[0]}_by_participants.csv"       
    df.loc['Total'] = df.sum()
    df['Duration'] = df['Duration'].apply(seconds_to_hms)
    df.to_csv(duration_by_particpants_output)
                


if __name__ == '__main__':
    # result_path = 'video_durations_bing.csv'
    # plot_result(result_path)
    # result_path = 'video_durations.csv'
    # plot_result(result_path)
    result_path = 'video_durations_local_repeat.csv'
    plot_result(result_path)
    result_path = 'video_durations_local.csv'
    plot_result(result_path)
