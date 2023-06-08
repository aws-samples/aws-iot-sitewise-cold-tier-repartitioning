# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
from datetime import datetime, timedelta
import time
import yaml
from itertools import repeat
from multiprocessing import cpu_count, Pool, freeze_support
import shutil
import helpers.s3 as s3_helper 
import helpers.sitewise as sitewise_helper

dir = os.path.abspath(os.path.dirname(__file__))
root_dir = os.path.abspath(os.path.dirname(dir))

# Load configuration
with open(f'{root_dir}/config.yml', 'r') as file:
    config = yaml.safe_load(file)

timeseries_type = config['timeseries_type']
cold_tier_bucket_name = config['s3_buckets']['cold_tier_bucket_name']
cold_tier_bucket_data_prefix = config['s3_buckets']['cold_tier_bucket_data_prefix']
repartitioned_bucket_name = config['s3_buckets']['repartitioned_bucket_name']
repartitioned_bucket_data_prefix = config['s3_buckets']['repartitioned_bucket_data_prefix']
repartitioned_bucket_index_prefix = config['s3_buckets']['repartitioned_bucket_index_prefix']
date_start = config['date_range']['date_start']
date_end = config['date_range']['date_end']
local_tmp_raw_dir_name = config['local_dirs']['local_tmp_raw_dir_name']
TMP_SITEWISE_PATH = '/tmp/sitewise'
local_tmp_raw_dir_path = f'{TMP_SITEWISE_PATH}/{local_tmp_raw_dir_name}'
   
def filter_keys(keys, all_timeseries_ids: list, previous_timeseries_ids: list) -> list:
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

def process_objects(filtered_keys: list, day_wise_folder: str) -> None:
    """Download the S3 files into day-wise directory
    """  
    download_start = time.time()

    #Download source objects from S3 Cold Tier to local day-wise directory
    print("\tDownloading S3 objects to local filesystem..")
    Pool(cpu_count() - 1).starmap(s3_helper.download_s3_object, zip(repeat(cold_tier_bucket_name), filtered_keys, repeat(day_wise_folder)))
    print(f'\t\t** Download time: {round(time.time() - download_start)} secs **')

def start() -> None:
    # Convert strings to datetime
    date_start_dt = datetime.strptime(date_start, '%Y-%m-%d').date()
    date_end_dt = datetime.strptime(date_end, '%Y-%m-%d').date()

    # Variable to loop through calender
    date_loop_dt = date_end_dt
    
    # Get the list of all relevant timeseries ids from SiteWise
    print(f'Starting to process timeseries data between {date_start_dt} and {date_end_dt}')
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

    # Loop through the configured time periodcd ..
    while date_loop_dt >= date_start_dt:
        s3_day_prefix = f'startYear={date_loop_dt.strftime("%Y")}/startMonth={date_loop_dt.month}/startDay={date_loop_dt.day}/'
        print(f'\nReviewing --> year: {date_loop_dt.year}, month: {date_loop_dt.month}, day: {date_loop_dt.day}')
        # Get a list of all s3 object keys for the day
        print(f'\tRetrieving all keys with prefix: {cold_tier_bucket_data_prefix}{s3_day_prefix}')
        s3_object_keys = s3_helper.get_all_s3_objects1(cold_tier_bucket_name, f'{cold_tier_bucket_data_prefix}{s3_day_prefix}')
 
        if len(s3_object_keys) == 0:
            print(f'\tNo Cold tier data! Skipping this day')
        else:
            day_wise_folder = f"{date_loop_dt.year}-{date_loop_dt.month}-{date_loop_dt.day}"
            raw_day_wise_folder_path = f"{local_tmp_raw_dir_path}/{day_wise_folder}"
            
            # Create daily directories if doesn't exist - moving this inside the IF statement fixed the bug when there was no data in provided partition
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
                process_objects(filtered_keys, day_wise_folder)
            else:
                print(f'\tSkip, no new data')
                # Remove any existing directory
                if os.path.exists(raw_day_wise_folder_path): shutil.rmtree(raw_day_wise_folder_path)
        
        date_loop_dt = date_loop_dt - timedelta(days=1)

if __name__ == "__main__":
    freeze_support()
    start()
    print('\nScript execution successfully completed!!')