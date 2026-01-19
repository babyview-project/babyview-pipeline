import os

from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def get_imu_data_lists_for_time_interval(
    imu_metadata_dir: str,
    time_start_sec: float,
    time_end_sec: float,
    context_history_sec: float = 2.0,   # ← renamed parameter
) -> Tuple[List[float], ...]:
    """
    Load IMU data in the interval
        [time_start_sec – context_history_sec,  time_end_sec]  (inclusive)

    Parameters
    ----------
    imu_metadata_dir : str
        Directory that contains (or will contain) the processed `imu.csv`.
    time_start_sec : float
        Start of the main window of interest, in seconds.
    time_end_sec : float
        End of the window of interest, in seconds.
    context_history_sec : float, optional
        Extra seconds of context *before* `time_start_sec` to include.
        Defaults to 0.0 (no extra history).

    Returns
    -------
    tuple of lists
        (timestamps,
         accl_x, accl_y, accl_z,
         gyro_x, gyro_y, gyro_z,
         grav_x, grav_y, grav_z)
    """
    # 1. Ensure the processed CSV exists
    process_imu_for_video_dir(imu_metadata_dir)

    # 2. Load CSV
    imu_csv_path = os.path.join(imu_metadata_dir, "imu.csv")
    df = pd.read_csv(imu_csv_path)

    # 3. Compute lower bound with context
    lower = max(0.0, time_start_sec - context_history_sec)

    # 4. Filter desired rows
    mask = (df["Timestamp (s)"] >= lower) & (df["Timestamp (s)"] <= time_end_sec)
    df = df.loc[mask]

    # 5. Extract columns as plain lists
    ts      = df["Timestamp (s)"].tolist()
    accl_x  = df["ACCL_X (m/s²)"].tolist()
    accl_y  = df["ACCL_Y (m/s²)"].tolist()
    accl_z  = df["ACCL_Z (m/s²)"].tolist()
    gyro_x  = df["GYRO_X (rad/s)"].tolist()
    gyro_y  = df["GYRO_Y (rad/s)"].tolist()
    gyro_z  = df["GYRO_Z (rad/s)"].tolist()
    grav_x  = df["GRAV_X (m/s²)"].tolist()
    grav_y  = df["GRAV_Y (m/s²)"].tolist()
    grav_z  = df["GRAV_Z (m/s²)"].tolist()

    return (
        ts,
        accl_x, accl_y, accl_z,
        gyro_x, gyro_y, gyro_z,
        grav_x, grav_y, grav_z,
    )

    
def plot_imu_signals(
    imu_data_lists: Tuple[list, ...],
    *,
    title: str = "IMU signals over selected time interval",
    time_start_sec: Optional[float] = None,
    figsize: tuple = (10, 6),
):
    """
    Plot all IMU channels versus time.

    Parameters
    ----------
    imu_data_lists : tuple of lists
        (ts, accl_x, accl_y, accl_z, gyro_x, gyro_y, gyro_z, grav_x, grav_y, grav_z)
    time_start_sec : float, optional
        If provided, a red vertical line is drawn at this x‑coordinate.
    """
    # ── unpack tuple ───────────────────────────────────────────────────────────
    (ts,
     accl_x, accl_y, accl_z,
     gyro_x, gyro_y, gyro_z,
     grav_x, grav_y, grav_z) = imu_data_lists

    # ── build plot ────────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=figsize)

    ax.plot(ts, accl_x, label="ACCL_X  (m/s²)")
    ax.plot(ts, accl_y, label="ACCL_Y  (m/s²)")
    ax.plot(ts, accl_z, label="ACCL_Z  (m/s²)")

    ax.plot(ts, gyro_x, label="GYRO_X  (rad/s)")
    ax.plot(ts, gyro_y, label="GYRO_Y  (rad/s)")
    ax.plot(ts, gyro_z, label="GYRO_Z  (rad/s)")

    ax.plot(ts, grav_x, label="GRAV_X  (m/s²)")
    ax.plot(ts, grav_y, label="GRAV_Y  (m/s²)")
    ax.plot(ts, grav_z, label="GRAV_Z  (m/s²)")

    # ── optional vertical & horizontal markers ───────────────────────────────
    if time_start_sec is not None:
        ax.axvline(
            x=time_start_sec,
            color="red",
            linestyle="--",
            linewidth=1.5,
        )

    # zero line for reference
    ax.axhline(
        y=0.0,
        color="red",
        linestyle="--",
        linewidth=1.0,
    )
    
    # ── cosmetics ─────────────────────────────────────────────────────────────
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Sensor value")
    ax.set_title(title)
    ax.legend(ncol=3, fontsize="small")
    ax.grid(True)
    fig.tight_layout()

    return fig, ax


def process_imu_for_video_dir(accel_dir):
    accel_txt_path = os.path.join(accel_dir, "ACCL_meta.txt")
    gyro_txt_path = os.path.join(accel_dir, "GYRO_meta.txt")
    grav_txt_path = os.path.join(accel_dir, "GRAV_meta.txt")
    
    accel_csv_path = os.path.join(accel_dir, f"accel.csv")
    gyro_csv_path = os.path.join(accel_dir, f"gyro.csv")
    grav_csv_path = os.path.join(accel_dir, f"grav.csv")

    # if os.path.exists(accel_csv_path) and os.path.exists(gyro_csv_path) and os.path.exists(grav_csv_path):
    #     print(f"IMU CSV files for {accel_dir} already exist. Skipping processing.")
    #     return

    # Convert txt files to CSV files
    if os.path.exists(accel_txt_path) and os.path.exists(gyro_txt_path) and os.path.exists(grav_txt_path):
        process_file(accel_txt_path, "ACCL", accel_csv_path)
        process_file(gyro_txt_path, "GYRO", gyro_csv_path)
        process_file(grav_txt_path, "GRAV", grav_csv_path)
    else:
        print(f"IMU txt files for {accel_dir} are missing.")
        return None

    # Load CSV data
    imu_data = load_csv_data(accel_csv_path, gyro_csv_path, grav_csv_path)
    if imu_data is not None:
        imu_df = pd.DataFrame(imu_data, columns=['Timestamp (s)', 
                                                 'ACCL_X (m/s²)', 'ACCL_Y (m/s²)', 'ACCL_Z (m/s²)', 
                                                 'GYRO_X (rad/s)', 'GYRO_Y (rad/s)', 'GYRO_Z (rad/s)',
                                                 'GRAV_X (m/s²)', 'GRAV_Y (m/s²)', 'GRAV_Z (m/s²)'])
        # imu_csv_path = os.path.join(accel_dir, "imu.csv")
        # imu_df.to_csv(imu_csv_path, index=False)
        # print(f"imu data saved to {imu_csv_path}")

    # delete the intermediate CSV files
    os.remove(accel_csv_path)
    os.remove(gyro_csv_path)
    os.remove(grav_csv_path)
    
    return imu_df

def process_file(file_path, sensor_type, output_file):
    # Attempt to read the file with different encodings
    encodings = ['utf-8', 'latin1', 'ISO-8859-1']
    file_content = None

    for encoding in encodings:
        try:
            with open(file_path, "r", encoding=encoding) as file:
                file_content = file.read()
            # print(f"Successfully read the file {file_path} with {encoding} encoding.")
            break
        except UnicodeDecodeError:
            pass
            # print(f"Failed to read the file {file_path} with {encoding} encoding.")

    if file_content is None:
        print(f"Failed to read the file {file_path} with all attempted encodings.")
        return

    _, _, data, sample_rates = parse_file(file_content, sensor_type)

    # Print the extracted data
    # print("Video Framerate:", video_framerate)
    # print("Payload Times:", payload_times)

    if sensor_type == "ACCL":
        columns = ["ACCL_X (m/s²)", "ACCL_Y (m/s²)", "ACCL_Z (m/s²)"]
    elif sensor_type == "GYRO":
        columns = ["GYRO_X (rad/s)", "GYRO_Y (rad/s)", "GYRO_Z (rad/s)"]
    elif sensor_type == "GRAV":
        columns = ["GRAV_X (m/s²)", "GRAV_Y (m/s²)", "GRAV_Z (m/s²)"]

    df = pd.DataFrame(data, columns=columns)
    # swap the values in GRAV_X and GRAV_Y
    # NOTE: Data order of ACCL/GYRO and GRAV seem to be different: https://github.com/langcog/babyview-pipeline/tree/main/meta_extract
    if sensor_type == "GRAV":
        df['GRAV_X (m/s²)'], df['GRAV_Y (m/s²)'] = df['GRAV_Y (m/s²)'], df['GRAV_X (m/s²)']
    # print(f"{sensor_type} Data:")
    # print(df)

    sample_rate_df = pd.DataFrame.from_dict(sample_rates, orient='index', columns=["Sampling Rate (Hz)", "Start Time (s)", "End Time (s)"])
    sample_rate_df.index.name = 'Sensor'
    # print("Sample Rates:")
    # print(sample_rate_df)

    # Compute timestamps and add to the data
    if sensor_type in sample_rates:
        # print(f"Computing timestamps for {sensor_type} data.")
        # print(f"Sample rate info: {sample_rates[sensor_type]}")
        timestamps = compute_timestamps(data, sample_rates[sensor_type])
        # print(f"Computed timestamps: {timestamps[:10]}")  # Print the first 10 timestamps for verification
        df["Timestamp (s)"] = timestamps

    # Save the data to a CSV file
    df.to_csv(output_file, index=False)
    # print(f"{sensor_type} data saved to {output_file}")
    
def parse_file(file_content, sensor_type):
    video_framerate = None
    payload_times = []
    data = []
    sample_rates = {}

    lines = file_content.split("\n")

    for i, line in enumerate(lines):
        line = line.strip()
        if line.startswith("VIDEO FRAMERATE:"):
            try:
                parts = lines[i+1].split()
                video_framerate = (float(parts[0]), int(parts[2]))
                # print(f"Matched video framerate: {video_framerate}")
            except (IndexError, ValueError) as e:
                print(f"Failed to parse video framerate: {lines[i+1]} ({e})")

        elif line.startswith("PAYLOAD TIME:"):
            try:
                parts = lines[i+1].split()
                payload_times.append((float(parts[0]), float(parts[2])))
                # print(f"Matched payload time: {payload_times[-1]}")
            except (IndexError, ValueError) as e:
                print(f"Failed to parse payload time: {lines[i+1]} ({e})")

        elif "sampling rate" in line:
            try:
                sensor, rest = line.split('sampling rate =')
                sensor = sensor.strip()
                rest_parts = rest.split()
                rate = float(rest_parts[0].replace('Hz', ''))
                start_time = float(rest_parts[2])
                end_time = float(rest_parts[4].strip(')",'))
                sample_rates[sensor] = (rate, start_time, end_time)
                # print(f"Matched sample rate: {sensor} with rate {rate}Hz from {start_time}s to {end_time}s")
            except (IndexError, ValueError) as e:
                print(f"Failed to parse sample rate: {line} ({e})")

        elif line.startswith(sensor_type):
            try:
                parts = line.replace(sensor_type, '').replace('m/s²', '').replace('rad/s', '').replace(',', '').split()
                data.append((float(parts[0]), float(parts[1]), float(parts[2])))
                # print(f"Matched {sensor_type.lower()} data: {data[-1]}")
            except (IndexError, ValueError) as e:
                print(f"Failed to parse {sensor_type.lower()} data: {line} ({e})")

    return video_framerate, payload_times, data, sample_rates

def compute_timestamps(data, sample_rate_info):
    rate, start_time, end_time = sample_rate_info
    num_samples = len(data)
    duration = end_time - start_time
    computed_rate = num_samples / duration
    
    # Check if the computed rate matches the provided rate within a tolerance
    tolerance = 0.01  # 1% tolerance
    if abs(computed_rate - rate) / rate > tolerance:
        raise ValueError(f"Computed rate {computed_rate}Hz does not match provided rate {rate}Hz within tolerance.")
    
    # Compute timestamps based on start_time and end_time
    timestamps = [start_time + i * duration / (num_samples - 1) for i in range(num_samples)]
    return timestamps

def load_csv_data(accel_path, gyro_path, grav_path):
    try:
        # Load accelerometer and gyroscope data
        accel_data = pd.read_csv(accel_path)
        gyro_data = pd.read_csv(gyro_path)
        grav_data = pd.read_csv(grav_path)
        
        # Merge based on the closest timestamps
        merged_data = pd.merge_asof(accel_data.sort_values('Timestamp (s)'), 
                                    gyro_data.sort_values('Timestamp (s)'), 
                                    on='Timestamp (s)')
        # grav data has lower sampling rate (30Hz) than accel and gyro (200Hz)
        merged_data = pd.merge_asof(
            grav_data.sort_values('Timestamp (s)'),
            merged_data.sort_values('Timestamp (s)'),
            on='Timestamp (s)',
            direction='nearest'
        )
        
        # interpolate to fill NaN values (of grav data, since it has lower sampling rate)
        merged_data = merged_data.interpolate(method='linear', limit_direction='both')
        
        # Create a unified IMU data array
        imu_data = np.zeros((len(merged_data), 10))
        imu_data[:, 0] = merged_data['Timestamp (s)']
        imu_data[:, 1:4] = merged_data[['ACCL_X (m/s²)', 'ACCL_Y (m/s²)', 'ACCL_Z (m/s²)']]
        imu_data[:, 4:7] = merged_data[['GYRO_X (rad/s)', 'GYRO_Y (rad/s)', 'GYRO_Z (rad/s)']]
        imu_data[:, 7:10] = merged_data[["GRAV_X (m/s²)", "GRAV_Y (m/s²)", "GRAV_Z (m/s²)"]]
        
        return imu_data
    except Exception as e:
        print(f"Error loading CSV data: {e}")
        return None