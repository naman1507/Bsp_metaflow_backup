import time
import influxdb_client
import string
import random
import pandas as pd
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb import DataFrameClient
from influxdb_client import InfluxDBClient, Point
from datetime import datetime, timedelta
import threading
import pickle

org = "iit_bh"
token = "pg0rBVHySellktOpSG2Ee103-wIvSOUBM_s0KXmR0mDWM60Kn0duN0jm6EudRIJ7ogpNs1JoAiEqtzcvGca70w=="
url = "http://127.0.0.1:8086"
bucket = "metaflow_test2"


client = InfluxDBClient(url=url, token=token)

write_api = client.write_api(write_options=SYNCHRONOUS)

measurement_name = "hmd_signals_transformed"

# Start index and end index
# This variables are used to find the start and end time for processing main buffer
start_index = None
end_index = None

# Define buffer time span (15 minutes)
buffer_time_span = timedelta(minutes = 15)

# Calculating Differences
# This calculates the differences for each continuous occurence of 0 and 1 in HMD signal
def calculate_Differences(col, df2):

    # Stores Output(This will be of same length as number of rows passed in buffer)
    output_list = []

    # Check if DataFrame is empty
    # If Dataframe is empty, empty list is returned
    if df2.empty:
        return output_list

    # To see the start value of column
    if df2[col][0] == 0.0:
        current_value = 0
    else:
        current_value = 1

    # Stores the count of values(continuous number of occurrence till new value is encountered)
    count = 0
    new_list = []
    ind = 0

    list1 = list(df2[col])

    # Finding the number of occurrence of each continuous value
    for val in list1:

        if val == current_value:
            count += 1

        else:
            new_list.append(count)
            current_value = val
            count = 1

        ind += 1

        # For Last index
        if ind == len(list1) - 1:
            new_list.append(count + 1)

    # Multiplying differences with itself(This will give the same length as column)
    for item in new_list:
        output_list.extend([item] * item)

    #print(output_list)

    # Returning Output List
    return output_list

# This function is for threading Since we want to send data for each column simultaneously
# This will call process_column_data function to process the data after buffer is of time frame or empty space is encountered
def add_data_to_influx(process_buffer):

    df = process_buffer

    threads = []

    for col_name in df.columns:

        thread = threading.Thread(target=process_column_data, args=(col_name, df))
        thread.start()
        threads.append(thread)

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

# Function to process data
# This will find differences and then append data to influxdb bucket
def process_column_data(col_name, df):

    # Since we are iterating the values of column and need time index
    val_ind_0 = 0
    val_ind_1 = 0

    # Calling Function
    Differences = calculate_Differences(col_name, df)

    # Iterating over column values and saving data in bucket
    for i, value in enumerate(df[col_name]):

        if value == 0.0:
            index = pd.to_datetime(df[df[col_name] == value].index[val_ind_0])
            val_ind_0 += 1
        else:
            index = pd.to_datetime(df[df[col_name] == value].index[val_ind_1])
            val_ind_1 += 1

        data_point = Point(measurement_name)
        data_point.time(index)
        data_point.tag('sensor_name', col_name)  # Use column name as sensor name
        data_point.field('sensor_reading', value)
        data_point.field('sensor_differences', Differences[i])

        # If value is 0 then tag name is green else orange for 1
        if value == 0.0:
            data_point.tag('sensor_color', "green")
        else:
            data_point.tag('sensor_color', "orange")

        # Writing data
        write_api.write(bucket=bucket, org=org, record=data_point)
        # Print the data point
        #print(f"Written data point: {data_point.to_line_protocol()}")


# Function to process data from buffer
# This will simply check if buffer has enough data for 15 minutes and then sent the data for processing
# This will call thread function and then it will call process function
def process_buffer_data(buffer):

  # To use throughout the program
  global start_index, end_index

  if end_index is None:

      end_index = main_buffer.index[-1]

  # Process data here
  #print("Processing data from", start_index, "to", end_index)
  add_data_to_influx(buffer)

  start_index = pd.to_datetime(end_index)
  end_index = None

# To check for empty data
# If current data index and prev index has a second difference of 1, no empty data
# Else empty data is present
prev_index = None

# This will add data to buffer
def add_data_to_buffer(index, row):

    global start_index, end_index, prev_index

    if start_index is None and prev_index is None:

        start_index = index
        prev_index = index

    # Adding to main buffer
    main_buffer.loc[index] = row

    if prev_index + timedelta(seconds = 1) != index:

        if start_index != index:

            process_buffer_data(main_buffer[prev_index: start_index])
            start_index = index

    # Check for buffer processing based on time difference
    if (index - start_index) >= buffer_time_span:
        process_buffer_data(main_buffer[start_index: index])
        start_index = index


    prev_index = index

    if main_buffer.index[-1] - main_buffer.index[0] == buffer_time_span:

        start_index = main_buffer.index[0]

        end_index = start_index + buffer_time_span

        start_index = pd.to_datetime(start_index)
        end_index = pd.to_datetime(end_index)

        process_buffer_data(main_buffer[start_index: end_index])

    else:

        if start_index is not None:

            if main_buffer.index[-1] - start_index == buffer_time_span:

                end_index = pd.to_datetime(start_index) + buffer_time_span

                process_buffer_data(main_buffer[start_index: end_index])

# Reading Data

filepath = r"/data2/naman/metaflow_inference/mat_absence_and_ghost_rolling_removed_march_05_24.pkl"
data = pd.read_pickle(filepath)

# Convert selected columns to float data type
data = data.astype(float)

# Selecting subset of HMD Signals
#data = data[['[1.182]', '[1.183]', '[1.193]']]

# List of HMD Signals
#hmd_signals = ['[1.182]', '[1.183]', '[1.193]']

columns_path= "/data2/naman/Jan_Feb'24/March/16mm/HMD/hmd_columns.pkl"

with open(columns_path, 'rb') as file:
    column_names = pickle.load(file)

data = data[column_names]
print(data.shape)

tagnames_df = pd.read_excel("/data2/naman/metaflow_inference/Finalised_region_combined_tagnames.xlsx")
merged_df = pd.merge(data.T, tagnames_df, left_index=True, right_on="Sensor_ID", how="left")
merged_df['Tagnames'] = merged_df['Tagnames'].fillna("Missing_TagName")
column_mapping = dict(zip(merged_df["Sensor_ID"], merged_df["Sensor_ID"]+'_'+merged_df["Tagnames"]))
data.rename(columns=column_mapping, inplace=True)

hmd_signals = data.columns
print(hmd_signals)

# Subset of data
#data = data['2024-03-05 00:27:02' : '2024-03-05 07:42:02']

# Buffer should be a dataframe
main_buffer = pd.DataFrame(columns=hmd_signals)

count = 1

# Sending data row by row for simulating real time data sending
for i, row in data.iterrows():

    #print(f"Row {count} Added with index {i} added.")

    # Add data to main buffer
    add_data_to_buffer(i, row)

    count += 1