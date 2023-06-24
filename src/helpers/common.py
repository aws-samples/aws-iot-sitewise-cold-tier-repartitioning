# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
from typing import List, Dict
import boto3

def visible_child_dirs(dir_path: str) -> List[str]:
    """List all visible child directories exclusing hidden files 
    like .DS_Store
    """
    dirs = [x for x in os.listdir(dir_path) if not x.startswith('.')]
    return dirs

def get_client(service_id: str):
    """Get boto3 client for the service provided
    """
    profile = os.environ.get('AWS_PROFILE')
    try:
        session = boto3.Session(profile_name=profile)
        client = session.client(service_id)
        return client
    except: raise Exception("\nFound an issue with credentials or region!")

def validate_config_inputs(config: Dict) -> None:
    """Validate all the user inputs provided in the config.yml file
    """
    timeseries_type = config['timeseries_type']
    s3_config = config['s3']
    glue_assets = s3_config['glue_assets']
    glue_assets_bucket = glue_assets['bucket_name']
    glue_assets_scripts_prefix = glue_assets['scripts_prefix']
    cold_tier_config = s3_config['cold_tier']
    cold_tier_bucket = cold_tier_config['bucket_name']
    cold_tier_data_prefix = cold_tier_config['data_prefix']
    repartitioned_config = s3_config['repartitioned']
    repartitioned_bucket = repartitioned_config['bucket_name']
    repartitioned_data_prefix = repartitioned_config['data_prefix']
    repartitioned_index_prefix = repartitioned_config['index_prefix']
    
    # IoT SiteWise
    if not timeseries_type or timeseries_type not in ('ASSOCIATED', 'DISASSOCIATED'): raise Exception("\nInvalid input for 'timeseries_type'") 
    # S3
    if not cold_tier_bucket: raise Exception("\nInvalid input for 's3.cold_tier.bucket_name'")  
    if not cold_tier_data_prefix or cold_tier_data_prefix.startswith('/') \
        or not cold_tier_data_prefix.endswith('raw/'): 
        raise Exception("\nInvalid input for 's3.cold_tier.data_prefix'") 
    if not repartitioned_bucket: raise Exception("\nInvalid input for 's3.repartitioned.bucket_name'")  
    if not repartitioned_data_prefix  or repartitioned_data_prefix.startswith('/') \
        or not repartitioned_data_prefix.endswith('/'): 
        raise Exception("\nInvalid input for 's3.repartitioned.data_prefix'") 
    if not repartitioned_index_prefix or repartitioned_index_prefix.startswith('/') \
        or not repartitioned_index_prefix.endswith('/'): 
        raise Exception("\nInvalid input for 's3.repartitioned.index_prefix'")
    # Glue
    if not glue_assets_bucket: raise Exception("\nInvalid input for 's3.glue_assets.bucket_name'")  
    if not glue_assets_scripts_prefix or glue_assets_scripts_prefix.startswith('/') \
            or not glue_assets_scripts_prefix.endswith('/'): 
        raise Exception("\nInvalid input for 's3.glue_assets.scripts_prefix'")