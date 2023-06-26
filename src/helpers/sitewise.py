# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
from typing import List, Dict
import time
from . import globals
from . import common as common_helper

dir = os.path.abspath(os.path.dirname(__file__))
root_dir = os.path.abspath(os.path.dirname(os.path.dirname(dir)))

# Load configuration
config = globals.config
timeseries_type = config['timeseries_type']

# Configure SiteWise client
sw_client = common_helper.get_client('iotsitewise')

def get_timeseries_ids(next_token: str) -> List[str]:
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

def get_all_timeseries_ids() -> List[str]:
    """Get list of timeseries ids from all the pages
    """
    timeseries_ids_all=[]
    has_more_records=True
    next_token=""
    
    while has_more_records:
        time.sleep(2)
        timeseries_ids,next_token = get_timeseries_ids(next_token)
        timeseries_ids_all.extend(timeseries_ids)
        has_more_records=False if next_token == "" else True
    
    return timeseries_ids_all