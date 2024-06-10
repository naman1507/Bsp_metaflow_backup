#########################################################################################################
####### this code is to fetch data from the Postgres database hosted in BSP site ######################## 
####### the code fetches data for 1 min and calls the metaflow workflow for that min ####################
####### also the 1 min data is stored in parquet format with the first timestamp as file name ###########
#########################################################################################################


import pandas as pd
import subprocess
import time
import os
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import argparse

# Set Metaflow environment variables for metaflow ui and service
#os.environ["METAFLOW_SERVICE_URL"] = "http://localhost:8080"
#os.environ["METAFLOW_DEFAULT_METADATA"] = "service"


# Read PostgreSQL connection parameters from environment variables else default will be used
POSTGRES_USER = os.getenv("POSTGRES_USER", "iitbsp")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "iitbsp")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "ibabsp")


# PostgreSQL connection parameters
#conn_str = 'postgresql://iitbsp:iitbsp@127.0.0.1:5432/ibabsp'
conn_str = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Create a SQLAlchemy engine
try:
    engine = create_engine(conn_str)
    print("Connection to DB ESTABLISHED..   ")
except Exception as e:
    print("Connection not established..", e)


table_name = 'ibadata'

# Function to save DataFrame to parquet
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
def call_workflow(file_path,profile):
    print("Calling subprocess...")
    try:
        profile_config = f"regions_info_{profile}.json"
        subprocess.run(["python3", "./loop.py", "run", "--data_path", file_path, "--config", profile_config], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with exit code: {e.returncode}")
    print("Subprocess completed.")

def get_initial_last_timestamp():
    query = f'SELECT MAX("i_time") FROM {table_name}'
    result = engine.execute(query).fetchone()
    if result[0] is not None:
        return result[0] - timedelta(minutes=1)
    else:
        return datetime.now() - timedelta(minutes=1)


def main(profile):
    last_timestamp = get_initial_last_timestamp()
    threshold_rows = 60
    query = f'SELECT * FROM {table_name} WHERE "i_time" > %s ORDER BY "i_time" LIMIT {threshold_rows}'
    start_time = time.time()
    count = 0
    tagnames_df = pd.read_excel("./Finalised_region_combined_tagnames.xlsx")
    mapping_dict = dict(zip(tagnames_df['Tagnames'], tagnames_df['Sensor_ID']))

    while True:
        # Fetch data from PostgreSQL
        while True:
            df = pd.read_sql_query(query, engine, params=(last_timestamp,))
            if len(df)== threshold_rows:
                break
            else:
                time.sleep(5)
        
        df.rename(columns={'i_time': 'Time'}, inplace=True)
        df.drop(columns=['local_time'], inplace=True)
        df.rename(columns=mapping_dict, inplace=True)
        df.replace({'f': 0, 't': 1}, inplace=True)
        df["[9:17]"] = df["[9:15]"]
        

        if 'index' in df.columns:
            df.drop(['index'], axis=1, inplace=True)
        
        # If there is no new data, wait for some time before trying again
        if df.empty:
            print("No new data found. Waiting for 1 minute...")
            time.sleep(60)
            count += 1
            if count == 2:
                break
            continue
        
        # Update the last timestamp processed
        last_timestamp = df['Time'].max()
        df.set_index('Time', inplace=True)
        timestamp = df.index[0]
        print("The df is ...", df)
        #print("In while loop...", df.head())

        save_to_parquet(df, timestamp)

                # Write the minute data to a temporary CSV file
        file_path = 'temp_data.csv'
        df.to_csv(file_path, index=True)

        # Call the workflow for the batch
        call_workflow(file_path, profile)

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
### command to run the file: python3 loop_main_with_bsp_db_1sec.py --profile 16mm ####################
### command to run the file: python3 loop_main.py --profile 20mm ####################
### command to run the file: python3 loop_main.py --profile 12mm ####################
### command to run the file: python3 loop_main.py --profile 10mm ####################
#####################################################################################