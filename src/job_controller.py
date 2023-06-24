# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import time
import math
import os
import yaml
from datetime import timedelta, datetime
import argparse
from typing import List, Dict
import helpers.glue as glue_helper
import helpers.common as common_helper

src_dir = os.path.abspath(os.path.dirname(__file__))
root_dir = os.path.abspath(os.path.dirname(src_dir))

# Load configuration
with open(f'{root_dir}/config.yml', 'r') as file:
    config = yaml.safe_load(file)
common_helper.validate_config_inputs(config)

class GlueJobRunner:
    def __init__(self, job_name_prefix, glue_role_arn, days_per_job, 
                 from_date, to_date, glue_assets_bucket, 
                 glue_assets_job_script_key, glue_assets_extra_py_key):
        self.job_name_prefix = job_name_prefix
        self.job_role = glue_role_arn
        self.glue_assets_job_script_key = glue_assets_job_script_key
        self.from_date = from_date
        self.to_date = to_date
        self.glue_assets_bucket = glue_assets_bucket
        self.days_per_job = int(days_per_job)
        self.glue_assets_extra_py_key = glue_assets_extra_py_key

    def run_jobs(self, job_id):
        response = glue_helper.run_job(
            JobName=job_id)
        
    def check_status(self, job_names):
        job_count = len(job_names)
        # Loop to check overall status periodically
        while True:
            job_run_ended_count = 0
            job_run_success_count = 0
            for job_name in job_names:
                job_status = glue_helper.get_job_run_status(job_name)
                if glue_helper.is_end_status(job_status): 
                    job_run_ended_count+=1
                    if job_status == 'SUCCEEDED': job_run_success_count+=1
            if job_count == job_run_ended_count: 
                print(f'\nEnded executing all {job_count} jobs!')
                break
            print(f'\tProgress: {job_run_ended_count} out of {job_count} jobs ended')
            time.sleep(30)
        print(f'Total Jobs: {job_count}, Successful Jobs: {job_run_success_count}')

    def create_jobs(self):
        days_per_job = self.days_per_job
        batch_timestamp = int(datetime.now().timestamp())
        
        while self.from_date <= self.to_date:
            from_date_str = self.from_date.strftime("%Y-%m-%d")
            if self.from_date < self.to_date and self.days_per_job > 1:
                to_date_str = (self.from_date + timedelta(days=days_per_job-1)).strftime("%Y-%m-%d")
            elif self.from_date < self.to_date and self.days_per_job == 1:
                to_date_str = from_date_str
            else:
                to_date_str = self.to_date.strftime("%Y-%m-%d")
            
            job_name = f"{self.job_name_prefix}-{batch_timestamp}-{from_date_str}-{to_date_str}"
            command = {
                    'Name': 'glueetl',
                    'ScriptLocation': f's3://{self.glue_assets_bucket}/{self.glue_assets_job_script_key}',
                    'PythonVersion': '3'
                } 
            default_arguments = {
                    '--job-language': 'python',
                    '--enable-metrics': '',
                    '--extra-py-files': f's3://{self.glue_assets_bucket}/{self.glue_assets_extra_py_key}',
                    '--additional-python-modules': 'python-snappy',
                    '--from-date': f"{from_date_str}",
                    '--to-date': f"{to_date_str}"
                }
            job_tags = {
                'source': 'sitewise-repartitioning'
            }
            response = glue_helper.create_job(job_name, self.job_role, command,
                default_arguments, job_tags, '4.0', 2, 'G.2X')

            print(f"\tCreated job {job_name}")
            glue_helper.start_job(job_name)
            time.sleep(2)
            self.from_date += timedelta(days=self.days_per_job)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('from_date', help='Start date')
    parser.add_argument('to_date', help='End date')
    parser.add_argument('days_per_job', help='Maximum days per ETL job')
    parser.add_argument('job_name_prefix', help='Prefix for job name')
    parser.add_argument('glue_role_arn', help='ARN of IAM role')
    args = parser.parse_args()
    
    script_start = time.time()

    ## Validate user inputs from makefile & command line arguments
    # Check if days_per_job is a non-zero positive integer
    if not args.days_per_job.isdigit() or int(args.days_per_job) == 0: raise Exception("\nInvalid input for 'days_per_job'")  
    # Check if from_date and to_date are in valid format
    try: 
        datetime.strptime(args.from_date, '%Y-%m-%d')
        datetime.strptime(args.to_date, '%Y-%m-%d')
    except ValueError: raise Exception("\nInvalid input for 'from_date' and/or 'to_date'")  
    # Check if full job name is UTF-8 strong and not more than 255 bytes long
    if len(f"{args.job_name_prefix}-{int(datetime.now().timestamp())}-{args.from_date}-{args.to_date}".encode('utf-8')) > 255: raise Exception("\nInvalid input for 'job_name_prefix'")  
    # Check if the glue_role_arn follows ARN format
    if not args.glue_role_arn.startswith('arn:aws:iam:'): raise Exception("\nInvalid input for 'glue_role_arn'")

    from_date_str = args.from_date
    to_date_str = args.to_date
    glue_assets = config['s3']['glue_assets']
    glue_assets_bucket = glue_assets['bucket_name']
    glue_assets_scripts_prefix = glue_assets['scripts_prefix']
        
    glue_assets_job_script_name = 'job_script.py'
    glue_assets_job_script_key = f'{glue_assets_scripts_prefix}{glue_assets_job_script_name}'
    glue_assets_extra_py_key = f'{glue_assets_scripts_prefix}helpers.zip'
    job_name_prefix = args.job_name_prefix
    glue_role_arn = args.glue_role_arn
    days_per_job = args.days_per_job

    from_date = datetime.strptime(from_date_str, '%Y-%m-%d')
    to_date = datetime.strptime(to_date_str, '%Y-%m-%d')
    total_days = (to_date - from_date).days + 1
    total_jobs = math.ceil(total_days/int(days_per_job))

    handler = GlueJobRunner(job_name_prefix,
                            glue_role_arn,
                            days_per_job,
                            from_date,
                            to_date,
                            glue_assets_bucket,
                            glue_assets_job_script_key,
                            glue_assets_extra_py_key)
    
    print(f'\nCreating {total_jobs} Glue ETL jobs to process data between {from_date_str} and {to_date_str}..')
    handler.create_jobs()
    
    print(f'** Total execution time: {round((time.time() - script_start))} seconds **')