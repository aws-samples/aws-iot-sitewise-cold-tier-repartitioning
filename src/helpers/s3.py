# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import yaml
import boto3

dir = os.path.abspath(os.path.dirname(__file__))
root_dir = os.path.abspath(os.path.dirname(os.path.dirname(dir)))

# Load configuration
with open(f'{root_dir}/config.yml', 'r') as file:
    config = yaml.safe_load(file)

cold_tier_bucket_name = config['s3_buckets']['cold_tier_bucket_name']
repartitioned_data_bucket_name = config['s3_buckets']['repartitioned_data_bucket_name']
local_tmp_raw_dir_name = config['local_dirs']['local_tmp_raw_dir_name']
local_tmp_raw_dir_path = f'/tmp/sitewise/{local_tmp_raw_dir_name}'

# Create an S3 client
session = boto3.Session(profile_name=config['credentials']['profile'])    
s3_client = session.client('s3')

def s3_prefix_exists(bucket: str, key: str) -> bool:
    """Check if a prefix exists
    """
    result = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
    return True if 'Contents' in result else False

def list_s3_objects(bucket: str, prefix: str, StartAfter: str) -> list[str]:
    """Get all S3 objects for the page
    """
    s3_object_keys=[]
 
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix,
        StartAfter=StartAfter)

    if 'Contents' in response:
        s3_object_keys = [obj["Key"] for obj in response["Contents"]]
    return s3_object_keys, response["IsTruncated"]

def get_all_s3_objects(bucket, prefix: str) -> list[str]:
    """Get all S3 objects for the provided prefix from all pages
    """
    s3_object_keys_all=[]
    has_more_records=True
    key_marker=""
    
    while has_more_records:
        s3_object_keys,has_more_records = list_s3_objects(bucket, prefix, key_marker)
        s3_object_keys_all.extend(s3_object_keys)
        if len(s3_object_keys) > 0:
            key_marker = s3_object_keys[-1]
        
    return s3_object_keys_all

def filename_from_key(key: str) -> str:
    """Extract the filename from the S3 key
    """
    tokens = key.split('/')
    file_name = tokens[len(tokens)-1]
    return file_name

def timeseries_id_from_key(key: str) -> str:
    """Extract timeseries id from the S3 key name
    """
    timeseries_id=""
    file_name = filename_from_key(key)
    timeseries_id = file_name.split('_')[1]
    return timeseries_id

def download_s3_objects(bucket: str, keys: list[str]) -> None:
    """Download S3 objects based on the provided keys
    """
    for key in keys:
        file_name = filename_from_key(key)
        with open(local_tmp_raw_dir_path + '/' + file_name, 'w+b') as f:
            s3_client.download_fileobj(bucket, key, f)
            
def download_s3_object(bucket: str, key: str, day_wise_folder: str) -> None:
    """Download S3 object for the provided key
    """
    file_name = filename_from_key(key)
    timeseries_id = timeseries_id_from_key(key)
    with open(local_tmp_raw_dir_path + '/' + day_wise_folder + '/' + file_name, 'w+b') as f:
        download_fileobj(bucket, key, f)
        
def download_fileobj(bucket: str, key: str, f) -> None:
    s3_client.download_fileobj(bucket, key, f)

def upload_file_to_s3(bucket: str, local_file_path: str, s3_key: str) -> None:
    s3_client.upload_file(local_file_path, bucket, s3_key)