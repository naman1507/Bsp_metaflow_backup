###################################################################################################################
####################### file to call the main loop.py file which contains metaflow code ###########################
#### this file reads data from the csv file and triggers a function call for each minute of data retrieved ########
############ also saves data for that minute in parquer format ###################################################
###################################################################################################################





import pandas as pd
import subprocess
import time
import os
import argparse

# Set Metaflow environment variables
#os.environ["METAFLOW_SERVICE_URL"] = "http://localhost:8080"
#os.environ["METAFLOW_DEFAULT_METADATA"] = "service"
"""
# Read your data into a DataFrame (replace this with your actual data loading logic)
from datetime import datetime
d_parser = lambda x : datetime.strptime(x,"%d.%m.%Y %H:%M:%S.%f")
# Read your data into a DataFrame (replace this with your actual data loading logic)
df = pd.read_csv('/data2/naman/metaflow/00.24.56_01.24.56.txt', sep='\t', parse_dates=['Time'], date_parser=d_parser)
#df.set_index('Time', inplace=True)
print(df.head())
"""



def save_to_parquet(df, timestamp):
    date_str = timestamp.strftime("%Y-%m-%d")
    time_str = timestamp.strftime("%H:%M:%S")
    hour_str = timestamp.strftime("%H")
    minute_str = timestamp.strftime("%M")
    
    directory = os.path.join("iba_data", date_str, hour_str)
    if not os.path.exists(directory):
        os.makedirs(directory)
        
    file_name = f"{time_str}.parquet"
    file_path = os.path.join(directory, file_name)
    df.to_parquet(file_path)
    print(f"Parquet file created successfully at {file_path}")

# Function to call the Metaflow workflow for a batch of data
def call_workflow(file_path, profile):
    print("Calling subprocess...")
    try:
        profile_config = f"regions_info_{profile}.json"
        subprocess.run(["python", "/data2/naman/metaflow_inference/loop.py", "run", "--data_path", file_path, "--config", profile_config], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with exit code: {e.returncode}")
    print("Subprocess completed.")


def main(profile):
    start_time = time.time()
    count = 0

    from datetime import datetime
    d_parser = lambda x : datetime.strptime(x,"%Y-%m-%d %H:%M:%S")
    df = pd.read_csv('./iba_data.txt', sep=',', parse_dates=['i_time'], date_parser=d_parser)
    
    
    tagnames_df = pd.read_excel("/data2/naman/metaflow_inference/Finalised_region_combined_tagnames.xlsx")
    mapping_dict = dict(zip(tagnames_df['Tagnames'], tagnames_df['Sensor_ID']))

    while True:
        # Fetch data from PostgreSQL
        batch_df = df[:60]
        batch_df.rename(columns={'i_time': 'Time'}, inplace=True)
        batch_df.drop(columns=['local_time'], inplace=True)
        batch_df.set_index('Time', inplace=True)
        batch_df.rename(columns=mapping_dict, inplace=True)
        batch_df['[9:17]'] = batch_df['[9:16]']
        batch_df.replace({'f': 0, 't': 1}, inplace=True)

        if 'index' in df.columns:
            batch_df.drop(['index'], axis=1, inplace=True)
        
        # If there is no new data, wait for some time before trying again
        if batch_df.empty:
            print("No new data found. Waiting for 1 minute...")
            time.sleep(60)
            count += 1
            if count == 2:
                break
            continue
        
        #print(batch_df)
        #print("In while loop...", df.head())
        timestamp = batch_df.index[0]
        save_to_parquet(batch_df, timestamp)

        # Write the minute data to a temporary CSV file
        file_path = 'temp_data.csv'
        batch_df.to_csv(file_path, index=True)

        # Call the workflow for the batch
        call_workflow(file_path, profile)

        df = df[60:]
        if len(df)<60:
            break

    end_time = time.time()
    total_time = end_time - start_time
    print(f"Total time taken: {total_time} seconds")

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--profile', type=str, help='Provide the current profile running...')

    args = parser.parse_args()

    profile = args.profile
   
    main(profile)


#####################################################################################
### command to run the file: python3 loop_main.py --profile 16mm ####################
### command to run the file: python3 loop_main.py --profile 20mm ####################
### command to run the file: python3 loop_main.py --profile 12mm ####################
### command to run the file: python3 loop_main.py --profile 10mm ####################
#####################################################################################
