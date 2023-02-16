# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import glob
import boto3
from datetime import datetime
import time
import yaml
from multiprocessing import cpu_count, Pool, freeze_support
import helpers.s3 as s3_helper 
import helpers.sitewise as sitewise_helper
import helpers.os as os_helper 
import shutil

dir = os.path.abspath(os.path.dirname(__file__))
root_dir = os.path.abspath(os.path.dirname(dir))

# Load configuration
with open(f'{root_dir}/config.yml', 'r') as file:
    config = yaml.safe_load(file)

repartitioned_data_bucket_name = config['s3_buckets']['repartitioned_data_bucket_name']
local_tmp_raw_dir_name = config['local_dirs']['local_tmp_raw_dir_name']
local_tmp_merged_dir_name = config['local_dirs']['local_tmp_merged_dir_name']
TMP_SITEWISE_PATH = '/tmp/sitewise'
local_tmp_raw_dir_path = f'{TMP_SITEWISE_PATH}/{local_tmp_raw_dir_name}'
local_tmp_merged_dir_path = f'{TMP_SITEWISE_PATH}/{local_tmp_merged_dir_name}'
    
def get_avro_file_name(day_directory):
    """Retrieve the AVRO file name from the merged day 
    directory
    """  
    file_name = None
    for name in os.listdir(day_directory):
        if name.endswith(".avro"): file_name = name
    return file_name
    
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
    s3_data_file_key_name = f'consolidated/{s3_day_prefix}{local_data_file_name}'
    s3_index_file_key_name = f'index/{s3_day_prefix}timeseries.txt'

    upload_start = time.time()

    s3_helper.upload_file_to_s3(repartitioned_data_bucket_name, local_data_file_path, s3_data_file_key_name)
    # Upload and overwrite if index file already exists in S3
    s3_helper.upload_file_to_s3(repartitioned_data_bucket_name, local_index_file_path, s3_index_file_key_name)
    print(f"{day_directory}: ** Upload Time: {round(time.time() - upload_start)} secs **")
    
    # Clean up the file system
    shutil.rmtree(f'{raw_folder_day_path}')
    shutil.rmtree(f'{merged_folder_day_path}')
    
if __name__ == "__main__":
    freeze_support()
    print(f"Started uploading re-partitioned AVRO data files and index file for each day")
    Pool(cpu_count() - 1).map(upload_to_repartitioned_data_s3_bucket,
    os_helper.visible_child_dirs(local_tmp_merged_dir_path))
    shutil.rmtree(f'{TMP_SITEWISE_PATH}')
    print("\nScript execution successfully completed!!")