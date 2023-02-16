# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import time
from datetime import datetime
import json
import yaml
import avro
import avro.schema
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
from multiprocessing import cpu_count, Pool, freeze_support
import shutil
import helpers.os as os_helper 

dir = os.path.abspath(os.path.dirname(__file__))
root_dir = os.path.abspath(os.path.dirname(dir))

script_start_timestamp = int(datetime.now().timestamp())

# Load configuration
with open(f'{root_dir}/config.yml', 'r') as file:
    config = yaml.safe_load(file)
local_tmp_raw_dir_name = config['local_dirs']['local_tmp_raw_dir_name']
local_tmp_merged_dir_name = config['local_dirs']['local_tmp_merged_dir_name']
TMP_SITEWISE_PATH = '/tmp/sitewise'
local_tmp_raw_dir_path = f'{TMP_SITEWISE_PATH}/{local_tmp_raw_dir_name}'
local_tmp_merged_dir_path = f'{TMP_SITEWISE_PATH}/{local_tmp_merged_dir_name}'

# Load AVRO schema to use for merging
with open(f'{root_dir}/avro_schema.json', 'r') as f:
    avro_schema = json.load(f)
schema_parsed = avro.schema.parse(json.dumps(avro_schema))

def prepare_merge_dirs() -> None:
    """Create daily directories if doesn't exist, else 
    empty the root directory
    """  
    if not os.path.exists(local_tmp_merged_dir_path): os.mkdir(local_tmp_merged_dir_path)
    else:  
        for child_dir in os_helper.visible_child_dirs(local_tmp_merged_dir_path):
            shutil.rmtree(f'{local_tmp_merged_dir_path}/{child_dir}')

def merge_s3_objects(day_directory: str) -> None:
    """Merge raw AVRO files for the day into a single file
    """    
    merge_start = time.time()
    tmp_raw_directory_path = local_tmp_raw_dir_path + "/" + day_directory
    tmp_merge_directory_path = local_tmp_merged_dir_path + "/" + day_directory
    
    # Create day directory for merging if doesn't exist
    if not os.path.exists(tmp_merge_directory_path): os.mkdir(tmp_merge_directory_path)

    merged_data_file_name = f'merged_series_{script_start_timestamp}.avro' 
    avro_writer = DataFileWriter(open(tmp_merge_directory_path + "/" + merged_data_file_name, "wb"), DatumWriter(), schema_parsed, codec='snappy')
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

if __name__ == "__main__":
    freeze_support()
    prepare_merge_dirs()
    print(f"Started merging AVRO data files and index files for each day")
    Pool(cpu_count() - 1).map(merge_s3_objects, os_helper.visible_child_dirs(local_tmp_raw_dir_path))
    print('\nScript execution successfully completed!!')