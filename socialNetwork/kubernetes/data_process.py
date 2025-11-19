#!/usr/bin/env python
# coding: utf-8

# In[163]:


from functools import reduce
from utils import interval_string_to_seconds

import argparse
import glob
import pandas as pd
import re
import os

# In[164]:

parser = argparse.ArgumentParser()
parser.add_argument("-t", "--time", default="10m")
parser.add_argument("-v", "--version", default="old")

args = parser.parse_args()

metric_folders = ["cpu_memory_"]
dfs = {folder: [] for folder in metric_folders}

print(f"Reading all metric folders in {os.getcwd()}")
for metric_folder_name in metric_folders:
    for filename in glob.glob(f"{metric_folder_name}/*.txt"):
        if not filename.endswith('rps.txt'):
            with open(filename, 'r') as file:
                lines = file.readlines()
                metric_values = []
                metric_names = []
                multiple_metric_values = {}
                replicas = []
                times = []
                thresholds = []

                multiple_metrics = metric_folder_name.count('_') > 1
                if multiple_metrics is True:
                    metric_names = list(filter(None, metric_folder_name.split("_")))

                # Parse each line
                for index, line in enumerate(lines[1:]):  # Skip the header line
                    parts = line.split()
                    # print(parts)
                    if index == 0 and parts[len(parts) - 1].startswith('5'):
                        continue
                    if args.version == "old":
                        if len(parts) == 7: # single metric
                            metric_match = re.search(r'(\d+)%/(\d+)%|(<unknown>)/(\d+)%', parts[2])
                            if metric_match:
                                metric_value = metric_match.group(1) or metric_match.group(3)
                                metric_value = int(metric_value) if metric_value and metric_value.isdigit() else None
                                if len(thresholds) == 0:
                                    threshold = metric_match.group(2) or metric_match.group(4)
                                    if len(thresholds) == 0:
                                        thresholds = [int(threshold)] if threshold else []
                            else:
                                metric_value = None
                            metric_values.append(metric_value)

                        else:
                            n_metrics = len(parts) - 6
                            thresholds = [None] * n_metrics
                            for metric_number in range(n_metrics):
                                metric_match = re.search(r'(\d+)%/(\d+)%|(<unknown>)/(\d+)%', parts[2 + metric_number])

                                if metric_match:
                                    metric_value = metric_match.group(1) or metric_match.group(3)
                                    metric_value = int(metric_value) if metric_value and metric_value.isdigit() else None
                                    if thresholds[n_metrics - 1] is None:
                                        threshold = metric_match.group(2) or metric_match.group(4)
                                        thresholds[metric_number] = int(threshold) if threshold else None
                                else:
                                    metric_value = None
                                if metric_names[metric_number] in multiple_metric_values.keys():
                                    multiple_metric_values[metric_names[metric_number]].append(metric_value)
                                else:
                                    multiple_metric_values[metric_names[metric_number]] = [metric_value]
                    elif args.version == "new":
                        n_metrics = (len(parts) - 6) // 2  # Calculate metrics based on new structure
                        thresholds = [None] * n_metrics

                        for metric_number in range(n_metrics):
                            # Extract metric name and value from paired elements
                            name_index = 2 + (2 * metric_number)
                            value_index = 3 + (2 * metric_number)

                            metric_name = parts[name_index].rstrip(':')  # Remove trailing colon
                            value_part = parts[value_index]

                            # Pattern matching for metric values
                            metric_match = re.search(r'(\d+)%/(\d+)%|(<unknown>)/(\d+)%', value_part)
                            if metric_match:
                                metric_value = metric_match.group(1) or metric_match.group(3)
                                metric_value = int(metric_value) if metric_value and metric_value.isdigit() else None
                                threshold = metric_match.group(2) or metric_match.group(4)
                                thresholds[metric_number] = int(threshold) if threshold else None
                            else:
                                metric_value = None

                            metric_names.append(metric_name)
                            metric_values.append(metric_value)
                            # Store in metric dictionary
                            if metric_name in multiple_metric_values:
                                multiple_metric_values[metric_name].append(metric_value)
                            else:
                                multiple_metric_values[metric_name] = [metric_value]
                    print(parts)
                    replica = int(parts[len(parts) - 2])
                    time = parts[len(parts) - 1]
                    replicas.append(replica)
                    times.append(time)

                if multiple_metrics is True:
                    metric_keys = []
                    for metric_number in range(n_metrics):
                        metric_keys.append(metric_names[metric_number] + "_" + filename.split('/')[1].split('.')[0])

                    metric_dict = {}
                    for index, key in enumerate(metric_keys):
                        metric_dict[key] = multiple_metric_values[metric_names[index]]
                        metric_dict[metric_names[index] + "_" + filename.split('/')[1].split('.')[0] + "_scaling_threshold"] = thresholds[index]

                    updated_dict = dict(metric_dict, **{f"replicas_{filename.split('/')[1].split('.')[0]}": replicas, 'time': times})

                    df = pd.DataFrame(updated_dict)
                else:
                    # print(metric_values, replicas, times, filename)
                    df = pd.DataFrame({
                        f"{metric_folder_name + filename.split('/')[1].split('.')[0]}": metric_values,
                        f"replicas_{filename.split('/')[1].split('.')[0]}": replicas,
                        'time': times
                    })

                    # Add the scaling threshold column
                    df[f"{metric_folder_name + filename.split('/')[1].split('.')[0]}_scaling_threshold"] = thresholds[0]
                # Display the DataFrame
                dfs[metric_folder_name].append(df)

print("Read successful.")


def generate_time_strings(start, end):
    time_strings = []
    total_seconds = end

    for seconds in range(start, total_seconds + 1, 15):
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        remaining_seconds = seconds % 60

        time_strings.append(format_time_string(hours, minutes, remaining_seconds))

    return time_strings

def parse_time_string(time_string):
    hours = 0
    minutes = 0
    seconds = 0

    # Parse the time string
    if 'h' in time_string:
        hours_part = time_string.split('h')[0]
        if hours_part:
            hours = int(hours_part)
        time_string = time_string.split('h')[1]

    # Handle cases where 'm' is absent
    if 'm' not in time_string:
        if 's' in time_string:
            # Case: only seconds are present
            seconds_part = time_string.replace('s', '')
            if seconds_part:
                seconds = int(seconds_part)
        else:
            # No 'm' or 's' in the time_string, assume it's all minutes
            if time_string:
                minutes = int(time_string)
    else:
        # Case: minutes and possibly seconds are present
        minutes_part = time_string.split('m')[0]
        if minutes_part:
            minutes = int(minutes_part)

        seconds_part = time_string.split('m')[1].replace('s', '')
        if seconds_part:
            seconds = int(seconds_part)

    return hours, minutes, seconds

def format_time_string(hours, minutes, seconds):
    if hours > 0:
        return f"{hours}h{minutes}m{seconds}s"
    else:
        return f"{minutes}m{seconds}s"

# round down if upto 5s; else round up to nearest 15s interval string
def round_to_nearest_15s_interval(time_string):
    hours, minutes, seconds = parse_time_string(time_string)

    # Convert to total seconds
    total_seconds = hours * 3600 + minutes * 60 + seconds

    # Round to nearest 15-second interval
    remainder = total_seconds % 15
    if remainder <= 5:
        rounded_seconds = total_seconds - remainder
    else:
        rounded_seconds = total_seconds + (15 - remainder)

    # Convert back to hours, minutes, and seconds
    new_hours = rounded_seconds // 3600
    rounded_seconds %= 3600
    new_minutes = rounded_seconds // 60
    new_seconds = rounded_seconds % 60

    return format_time_string(new_hours, new_minutes, new_seconds)

def increment_time_by_15s(time_string):
    hours, minutes, seconds = parse_time_string(time_string)

    # Convert to total seconds
    total_seconds = hours * 3600 + minutes * 60 + seconds + 15  # Add 15 seconds

    # Convert back to hours, minutes, and seconds
    new_hours = total_seconds // 3600
    total_seconds %= 3600
    new_minutes = total_seconds // 60
    new_seconds = total_seconds % 60

    return format_time_string(new_hours, new_minutes, new_seconds)

# old, without hours:
# def generate_time_strings(start, end):
#     time_strings = []
#     total_seconds = end * 60  # minutes in seconds
#
#     for seconds in range(start * 60, total_seconds + 1, 15):
#         minutes = seconds // 60
#         remaining_seconds = seconds % 60
#         time_string = f"{minutes}m{remaining_seconds}s"
#         time_strings.append(time_string)
#
#     return time_strings
#
# # round down if upto 5s; else round up to nearest 15s interval string
# def round_to_nearest_15s_interval(time_string):
#     # Handle cases where 'm' is absent
#     if 'm' not in time_string:
#         time_string = '0m' + time_string
#
#     # Extract minutes and seconds from the input string
#     parts = time_string.replace('s', '').split('m')
#     minutes = int(parts[0])
#     seconds = int(parts[1]) if parts[1] else 0
#
#     # Convert to total seconds
#     total_seconds = minutes * 60 + seconds
#
#     # Round to nearest 15-second interval
#     remainder = total_seconds % 15
#     if remainder <= 5:
#         rounded_seconds = total_seconds - remainder
#     else:
#         rounded_seconds = total_seconds + (15 - remainder)
#
#     # Convert back to minutes and seconds
#     new_minutes = rounded_seconds // 60
#     new_seconds = rounded_seconds % 60
#
#     # Format and return the result
#     return f"{new_minutes}m{new_seconds}s"
#
# def increment_time_by_15s(time_string):
#     parts = time_string.replace('s', '').split('m')
#     minutes = int(parts[0])
#     seconds = int(parts[1]) if parts[1] else 0
#
#     total_seconds = minutes * 60 + seconds + 15
#     new_minutes = total_seconds // 60
#     new_seconds = total_seconds % 60
#
#     return f"{new_minutes}m{new_seconds}s"
#
# expected_timestamps = generate_time_strings(60, interval_string_to_seconds(args.time))
#
# print("Rounding timestamps to nearest 15s interval. Round down for upto 5s difference; else round up.")
# # round timestamps to nearest 15s timestamp
# for key in dfs.keys():
#     for index in range(len(dfs[key])):
#         df = dfs[key][index]
#         df['time'] = df['time'].apply(lambda x: round_to_nearest_15s_interval(x))
#         # Handle duplicates and maintain sequence for double-digit minute values
#         base_time = None
#         last_valid_time = None
#         for i in range(len(df)):
#             current_time = df.at[i, 'time']
#             # minutes = int(current_time.split('m')[0])
#
#             # if minutes >= 10:
#             if base_time is None:
#                 base_time = current_time
#                 last_valid_time = current_time
#             elif current_time <= last_valid_time:
#                 df.at[i, 'time'] = increment_time_by_15s(last_valid_time)
#                 last_valid_time = df.at[i, 'time']
#             else:
#                 last_valid_time = current_time
#             # else:
#             #     base_time = None
#             #     last_valid_time = None
#
# # insert rows for constant cpu utilization (HPA only shows entries for when the utilization changes)
# def insert_row_at_index(df, new_row, index):
#     # Create a DataFrame from the new row
#     new_row_df = pd.DataFrame([new_row], columns=df.columns)
#
#     # Insert the new row at the specified index
#     df_result = pd.concat([df.iloc[:index], new_row_df, df.iloc[index:]]).reset_index(drop=True)
#
#     return df_result
#
# print("Inserting missing rows where Δutil = 0.")
# for key in dfs.keys():
#     for df_index in range(len(dfs[key])):
#         df = dfs[key][df_index]
#         new_df = pd.DataFrame(columns=df.columns)
#
#         df_row_index = 0
#         for expected_timestamp in expected_timestamps:
#             if df_row_index < len(df) and df.iloc[df_row_index]['time'] == expected_timestamp:
#                 new_df = pd.concat([new_df, df.iloc[[df_row_index]]])
#                 df_row_index += 1
#             else:
#                 new_row = new_df.iloc[-1].copy() if len(new_df) > 0 else df.iloc[0].copy()
#                 new_row['time'] = expected_timestamp
#                 new_df = pd.concat([new_df, pd.DataFrame([new_row])])
#
#         dfs[key][df_index] = new_df.reset_index(drop=True)

rps = {folder: [] for folder in metric_folders}

print("Reading RPS files.")
for metric_folder_name in metric_folders:
    with open(metric_folder_name + "/rps.txt", 'r') as file:
        lines = file.readlines() # [:41]
        for line in lines:
            cols = list(filter(lambda x: ":" in x, line.split(";")))
            keys = list(map(lambda x: x.split(":")[0], cols))
            vals = list(map(float, map(lambda x: x.split(":")[1], cols)))
            row_data = dict(zip(keys, vals))
            rps[metric_folder_name].append(row_data)
print("RPS files read successfully.")

rps_dfs = {r: pd.DataFrame(rps[r]) for r in rps.keys()}

print("Merging dataframes for all microservices into one dataframe.")
# merge dataframes for all microservices into one dataframe
# print(rps_dfs["cpu_"])
# print(dfs["cpu_"][0])
# merge_dfs = lambda dfs_list: reduce(lambda left, right: pd.merge(left, right, on='time', how='outer'), dfs_list)
merge_dfs = lambda dfs_list: reduce(lambda left, right: pd.merge(
    left.drop('time', axis=1), right, left_index=True, right_index=True, how='inner'), dfs_list)
print("Dataframe merge successful.")

def custom_sort_key(x):
    try:
        return pd.Timedelta(x).total_seconds()
    except:
        return float(x)

combined_microservices_dataframes = {}

print("Merging HPA and RPS dataframes.")
for metric_folder_name in metric_folders:
    combined_microservices_dataframes[metric_folder_name] = merge_dfs(dfs[metric_folder_name]) # timestamps are lexicographically sorted
    combined_microservices_dataframes[metric_folder_name] = combined_microservices_dataframes[metric_folder_name].sort_values('time', key=lambda x: x.map(custom_sort_key))
    combined_microservices_dataframes[metric_folder_name] = combined_microservices_dataframes[metric_folder_name].reset_index(drop=True)
print("HPA and RPS merge successful.")

datasets = {}

dataset_type = "test" if args.time == "10m" else "train"

print("Writing datasets to disk.")
for metric_folder_name in metric_folders:
    datasets[metric_folder_name] = pd.concat([combined_microservices_dataframes[metric_folder_name], rps_dfs[metric_folder_name]], axis=1)
    datasets[metric_folder_name].to_csv(f"{metric_folder_name}/{metric_folder_name}{dataset_type}.csv", index=False)
print("Datasets written successfully.")
