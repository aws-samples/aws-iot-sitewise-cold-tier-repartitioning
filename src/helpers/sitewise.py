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
timeseries_type = config['timeseries_type']

# Configure SiteWise client
session = boto3.Session(profile_name=config['credentials']['profile'])    
sw_client = session.client('iotsitewise')

def get_timeseries_ids(next_token: str) -> list[str]:
    """Get list of timeseries ids for the page
    """
    if len(next_token) > 0:
        response = sw_client.list_time_series(timeSeriesType=timeseries_type, maxResults=50, nextToken=next_token)
    else:
        response = sw_client.list_time_series(timeSeriesType=timeseries_type, maxResults=50)
        
    #print(f'Total number of associated time series: {len(response["TimeSeriesSummaries"])}')
    timeseries_ids = [series["timeSeriesId"] for series in response["TimeSeriesSummaries"]]
    
    if "nextToken" in response:
        #print(f'{next_token}, {type(response["nextToken"])}')
        return timeseries_ids, response["nextToken"]
    else: 
        return timeseries_ids, ""

def get_all_timeseries_ids() -> list[str]:
    """Get list of timeseries ids from all the pages
    """
    timeseries_ids_all=[]
    has_more_records=True
    next_token=""
    
    while has_more_records:
        timeseries_ids,next_token = get_timeseries_ids(next_token)
        timeseries_ids_all.extend(timeseries_ids)
        has_more_records=False if next_token == "" else True
    
    return timeseries_ids_all