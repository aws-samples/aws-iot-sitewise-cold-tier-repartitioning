# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import sys
from datetime import datetime, timedelta
import time
import json
import avro
import avro.schema
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
import shutil
from typing import List, Dict
import helpers.common as common_helper 
import helpers.s3 as s3_helper 
import helpers.sitewise as sitewise_helper
import helpers.globals as globals
from itertools import repeat
from multiprocessing import cpu_count, Pool, freeze_support
from awsglue.utils import getResolvedOptions

dir = os.path.abspath(os.path.dirname(__file__))
root_dir = os.path.abspath(os.path.dirname(dir))

# Get Glue job arguments
args = getResolvedOptions(sys.argv, ['from-date', 'to-date'])
# Load config yaml
config = globals.config
# Load AVRO schema to use for merging
avro_schema = globals.avro_schema
avro_schema_parsed = avro.schema.parse(json.dumps(avro_schema))

timeseries_type = config['timeseries_type']
cold_tier_bucket_name = config['s3']['cold_tier']['bucket_name']
cold_tier_bucket_data_prefix = config['s3']['cold_tier']['data_prefix']
repartitioned_bucket_name = config['s3']['repartitioned']['bucket_name']
repartitioned_bucket_data_prefix = config['s3']['repartitioned']['data_prefix']
repartitioned_bucket_index_prefix = config['s3']['repartitioned']['index_prefix']

date_start = args['from_date']
date_end = args['to_date']
local_tmp_raw_dir_name = 'raw'
local_tmp_merged_dir_name = 'merged'
TMP_SITEWISE_PATH = '/tmp/sitewise'
local_tmp_raw_dir_path = f'{TMP_SITEWISE_PATH}/{local_tmp_raw_dir_name}'
local_tmp_merged_dir_path = f'{TMP_SITEWISE_PATH}/{local_tmp_merged_dir_name}'

script_start_timestamp = int(datetime.now().timestamp())

def filter_keys(keys, all_timeseries_ids: List[str], previous_timeseries_ids: List[str]) -> List[str]:
    """Filter S3 keys whose timeseries is 1/ part of the target 
    timeseries list and 2/ not previously processed for the day
    """
    filtered_keys = []
    new_timeseries_ids = []
    for key in keys:
        timeseries_id = s3_helper.timeseries_id_from_key(key)
        if timeseries_id in all_timeseries_ids and timeseries_id not in previous_timeseries_ids:
            filtered_keys.append(key)
            new_timeseries_ids.append(timeseries_id) if timeseries_id not in new_timeseries_ids else None
    
    return filtered_keys, new_timeseries_ids

def get_avro_file_name(day_directory) -> str:
    """Retrieve the AVRO file name from the merged day 
    directory
    """  
    file_name = None
    for name in os.listdir(day_directory):
        if name.endswith(".avro"): file_name = name
    return file_name

def download_objects(filtered_keys: List[str], day_wise_folder: str) -> None:
    """Download the S3 files into day-wise directory
    """  
    download_start = time.time()
    #Download source objects from S3 Cold Tier to local day-wise directory
    print(f"\tDownloading S3 objects..")
    Pool(cpu_count() - 1).starmap(s3_helper.download_s3_object, zip(repeat(cold_tier_bucket_name), filtered_keys, repeat(day_wise_folder)))
    print(f'\t\t** Download time: {round(time.time() - download_start)} secs **')

def merge_s3_objects(day_directory: str) -> None:
    """Merge raw AVRO files for the day into a single file
    """
    #Create daily directories if doesn't exist, else empty the root directory
    if not os.path.exists(local_tmp_merged_dir_path): os.mkdir(local_tmp_merged_dir_path)
    else:  
        for child_dir in common_helper.visible_child_dirs(local_tmp_merged_dir_path):
            shutil.rmtree(f'{local_tmp_merged_dir_path}/{child_dir}')

    merge_start = time.time()
    print(f"\tStarted merging AVRO data files and index files for each day..")
    tmp_raw_directory_path = local_tmp_raw_dir_path + "/" + day_directory
    tmp_merge_directory_path = local_tmp_merged_dir_path + "/" + day_directory
    
    # Create day directory for merging if doesn't exist
    if not os.path.exists(tmp_merge_directory_path): os.mkdir(tmp_merge_directory_path)

    merged_data_file_name = f'merged_series_{script_start_timestamp}.avro' 
    avro_writer = DataFileWriter(open(tmp_merge_directory_path + "/" + merged_data_file_name, "wb"), DatumWriter(), avro_schema_parsed, codec='snappy')
    index_file = ''
    
    # Loop through each file in the day directory
    for target_file in os.listdir(tmp_raw_directory_path):
        file_path = tmp_raw_directory_path + "/" + target_file
        # Merge AVRO files into one file
        if target_file.endswith(".avro"):
            reader = DataFileReader(open(file_path, "rb"), DatumReader())
            records = [record for record in reader]
            reader.close()
            for record in records:
                avro_writer.append(record)
        # Combine timeseries ids from index files
        if target_file.endswith(".txt"):
            with open(file_path, 'r') as f:
                file_content = f.read()
            index_file += file_content if not index_file else f'\n{file_content}'
    
    avro_writer.close()

    # Write combined timeseries ids to a single index file
    if index_file:
        with open(tmp_merge_directory_path + '/timeseries.txt', 'w') as f:
            f.write(index_file)

    print(f'{day_directory}: ** Merge Time: {round(time.time() - merge_start)} secs **')

def upload_to_repartitioned_data_s3_bucket(day_directory: str) -> None:
    """Upload merged files to the new S3 bucket
    """  
    folder_date = datetime.strptime(day_directory, '%Y-%m-%d').date()
    s3_day_prefix = f'startYear={folder_date.strftime("%Y")}/startMonth={folder_date.month}/startDay={folder_date.day}/'
    
    # Configure local file names and paths
    raw_folder_day_path = local_tmp_raw_dir_path + "/" + day_directory
    merged_folder_day_path = local_tmp_merged_dir_path + "/" + day_directory

    local_data_file_name = get_avro_file_name(merged_folder_day_path)
    if not local_data_file_name: return False
    local_data_file_path = f'{merged_folder_day_path}/{local_data_file_name}'
    local_index_file_path = merged_folder_day_path + "/timeseries.txt"
    
    # Configure S3 keys
    s3_data_file_key_name = f'{repartitioned_bucket_data_prefix}{s3_day_prefix}{local_data_file_name}'
    s3_index_file_key_name = f'{repartitioned_bucket_index_prefix}{s3_day_prefix}timeseries.txt'

    upload_start = time.time()

    print(f"Started uploading re-partitioned AVRO data files and index file for each day")
    s3_helper.upload_file_to_s3(repartitioned_bucket_name, local_data_file_path, s3_data_file_key_name)
    # Upload and overwrite if index file already exists in S3
    s3_helper.upload_file_to_s3(repartitioned_bucket_name, local_index_file_path, s3_index_file_key_name)
    print(f"{day_directory}: ** Upload Time: {round(time.time() - upload_start)} secs **")

def process_day(filtered_keys: List[str], day_wise_folder: str) -> None:
    """Process the data for the given day
    """  
    download_objects(filtered_keys, day_wise_folder)
    merge_s3_objects(day_wise_folder)
    upload_to_repartitioned_data_s3_bucket(day_wise_folder)

def start() -> None:
    """Start the execution
    """  
    # Convert strings to datetime
    date_start_dt = datetime.strptime(date_start, '%Y-%m-%d').date()
    date_end_dt = datetime.strptime(date_end, '%Y-%m-%d').date()

    # Variable to loop through calender
    date_loop_dt = date_end_dt
    print(f'Starting to process timeseries data between {date_start_dt} and {date_end_dt}')

    # Get the list of all relevant timeseries ids from SiteWise
    all_timeseries_ids = sitewise_helper.get_all_timeseries_ids()
    print(f'Configured timeseries mode: {timeseries_type}')
    print(f'Configured cold tier bucket name: {cold_tier_bucket_name}')
    print(f'Configured cold tier bucket data prefix: {cold_tier_bucket_data_prefix}')
    print(f'Configured repartitioned bucket name: {repartitioned_bucket_name}')
    print(f'Configured repartitioned bucket data prefix: {repartitioned_bucket_data_prefix}')
    print(f'Configured repartitioned bucket index prefix: {repartitioned_bucket_index_prefix}')
    print(f'Total timeseries identified: {len(all_timeseries_ids)}')
 
    # Create daily directories if doesn't exist
    if not os.path.exists(TMP_SITEWISE_PATH): os.mkdir(TMP_SITEWISE_PATH)
    if not os.path.exists(local_tmp_raw_dir_path): os.mkdir(local_tmp_raw_dir_path)

    # Loop through the configured time period
    while date_loop_dt >= date_start_dt:
        s3_day_prefix = f'startYear={date_loop_dt.strftime("%Y")}/startMonth={date_loop_dt.month}/startDay={date_loop_dt.day}/'
        print(f'\nReviewing --> year: {date_loop_dt.year}, month: {date_loop_dt.month}, day: {date_loop_dt.day}')
        
        # Get a list of all s3 object keys for the day
        print(f'\tRetrieving all keys with prefix: {cold_tier_bucket_data_prefix}{s3_day_prefix}')
        s3_object_keys = s3_helper.get_all_s3_objects(cold_tier_bucket_name, f'{cold_tier_bucket_data_prefix}{s3_day_prefix}')

        if len(s3_object_keys) == 0:
            print(f'\tNo Cold tier data! Skipping this day')
        else:
            day_wise_folder = f"{date_loop_dt.year}-{date_loop_dt.month}-{date_loop_dt.day}"
            raw_day_wise_folder_path = f"{local_tmp_raw_dir_path}/{day_wise_folder}"

            # Create daily directories if doesn't exist
            if not os.path.exists(raw_day_wise_folder_path): os.mkdir(raw_day_wise_folder_path)  
        
            previous_index_key = f'{repartitioned_bucket_index_prefix}{s3_day_prefix}timeseries.txt'
            previous_timeseries_ids = []
        
            # Download and read previous index file for the day, if exists
            if s3_helper.s3_prefix_exists(repartitioned_bucket_name, previous_index_key):
                previous_index_local_path = local_tmp_raw_dir_path + '/' + day_wise_folder + '/timeseries-previous.txt'
                with open(previous_index_local_path, 'w+b') as f1:
                    s3_helper.download_fileobj(repartitioned_bucket_name, previous_index_key, f1)
                with open(previous_index_local_path, 'r') as f2:
                    previous_timeseries_ids = f2.read().splitlines()
                print(f'\t# of timeseries previously processed: {len(previous_timeseries_ids)}')

            filtered_keys, new_timeseries_ids = filter_keys(s3_object_keys, all_timeseries_ids, previous_timeseries_ids)

            # Create local index for newly detected timeseries
            new_timeseries_count = len(new_timeseries_ids)
            if new_timeseries_count > 0:
                print(f'\t# of new timeseries detected: {new_timeseries_count}')
                with open(local_tmp_raw_dir_path + '/' + day_wise_folder + '/timeseries-new.txt', 'w') as f:
                    f.write('\n'.join(new_timeseries_ids))

            # Start processing objects
            if len(filtered_keys) > 0:
                print(f'\tFound new data to process, starting to download')
                process_day(filtered_keys, day_wise_folder)
            else:
                print(f'\tSkip, no new data')
                # Remove any existing directory for the day
                if os.path.exists(raw_day_wise_folder_path): shutil.rmtree(raw_day_wise_folder_path)

        date_loop_dt = date_loop_dt - timedelta(days=1)

if __name__ == "__main__":
    freeze_support()
    start()
    print('\nCleaning up the file system..')
    shutil.rmtree(f'{TMP_SITEWISE_PATH}')
    print('\nScript execution successfully completed!!')