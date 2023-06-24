# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from typing import List, Dict
from . import common as common_helper

# Create a Boto3 Glue client
glue_client = common_helper.get_client('glue')

def create_job(job_name: str, job_role: str, command: Dict, default_arguments: Dict, tags: Dict, glue_version: str, worker_count: int, worker_type: str) -> None:
    """Create a job based on configuration provided
    """
    glue_client.create_job(
                    Name=job_name,
                    Role=job_role,
                    Command=command,
                    DefaultArguments=default_arguments,
                    Tags=tags,
                    GlueVersion=glue_version,
                    NumberOfWorkers=worker_count,
                    WorkerType=worker_type
                )
            
def start_job(job_name: str) -> None:
    """Start the job based on job name provided
    """
    glue_client.start_job_run(JobName = job_name)

def delete_job(job_name: str) -> None:
    """Delete the job based on job name provided
    """
    glue_client.delete_job(JobName = job_name)

def get_job_runs(job_name: str) -> List[Dict]:
    """Get list of job runs for the job name provided
    """
    job_run_list = []
    # Create a Glue paginator
    paginator = glue_client.get_paginator('get_job_runs')
    # Define the paginator parameters
    paginator_params = { 'JobName': job_name }

    for page in paginator.paginate(**paginator_params):
        job_runs = page['JobRuns']
        job_run_list.extend(job_runs)
    return job_run_list

def get_job_run_status(job_name: str) -> str:
    """Get the status of job run
    """
    job_runs = get_job_runs(job_name)
    job_run_status = None
    # Only considering the first run
    if len(job_runs) > 0: job_run_status = job_runs[0]["JobRunState"]
    return job_run_status

def is_end_status(run_status: str) -> bool:
    """Check if the status represents an end status
    """
    return True if run_status in ('STOPPED', 'SUCCEEDED', 'FAILED',
                                       'ERROR', 'TIMEOUT') else False

def get_all_job_names() -> List[str]:
    """Get list of all job names
    """
    all_jobs_names = []
    # Set the initial value of the NextToken parameter to None
    next_token = ''
    while True:
        response = glue_client.list_jobs(
            MaxResults=1000,
            Tags = { 'source': 'sitewise-repartitioning' },
            NextToken=next_token
            )
        all_jobs_names.extend(response['JobNames'])
    
        # Check if there is a NextToken in the response
        if 'NextToken' in response:
            next_token = response['NextToken']
        else:
            # If there is no NextToken, exit the loop
            break
    return all_jobs_names