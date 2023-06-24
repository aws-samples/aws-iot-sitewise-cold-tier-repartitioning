# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import yaml
import shutil
import helpers.common as common_helper
import helpers.s3 as s3_helper

src_dir = os.path.abspath(os.path.dirname(__file__))
root_dir = os.path.abspath(os.path.dirname(src_dir))

# Load configuration
with open(f'{root_dir}/config.yml', 'r') as file:
    config = yaml.safe_load(file)
common_helper.validate_config_inputs(config)

s3_glue_assets = config['s3']['glue_assets']
s3_glue_assets_bucket = s3_glue_assets['bucket_name']
s3_glue_assets_scripts_prefix = s3_glue_assets['scripts_prefix']
s3_glue_assets_job_script_name = 'job_script.py'
local_job_helpers_dir_path = f'{src_dir}/helpers'
local_job_helpers_zip_path = f'{src_dir}/helpers.zip'

s3_helper.upload_file_to_s3(s3_glue_assets_bucket, f'{src_dir}/job_script.py', f'{s3_glue_assets_scripts_prefix}{s3_glue_assets_job_script_name}')
if os.path.exists(f'{local_job_helpers_dir_path}/__pycache__'): shutil.rmtree(f'{local_job_helpers_dir_path}/__pycache__')
if os.path.exists(f'{local_job_helpers_dir_path}/.DS_Store'): os.remove(f'{local_job_helpers_dir_path}/.DS_Store')
shutil.make_archive(f'{src_dir}/helpers', 'zip', src_dir, 'helpers')
s3_helper.upload_file_to_s3(s3_glue_assets_bucket, local_job_helpers_zip_path, f'{s3_glue_assets_scripts_prefix}helpers.zip')
if os.path.exists(local_job_helpers_zip_path): os.remove(local_job_helpers_zip_path)