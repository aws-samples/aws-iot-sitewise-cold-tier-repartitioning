# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import time
import helpers.glue as glue_helper 

src_dir = os.path.abspath(os.path.dirname(__file__))
root_dir = os.path.abspath(os.path.dirname(src_dir))

all_job_names = glue_helper.get_all_job_names()

print(f'Removing all ended jobs..')
ended_count=0
skipped_count=0
for job_name in all_job_names:
    status = glue_helper.get_job_run_status(job_name)
    ended = glue_helper.is_end_status(status)
    if ended: 
        glue_helper.delete_job(job_name)
        print(f'\tREMOVED - Name: {job_name}')
        ended_count+=1
    else:
        print(f'\tSKIPPED - Name: {job_name}')
        skipped_count+=1
    time.sleep(1)
    
print(f'Removed {ended_count} ended jobs')
print(f'Skipped {skipped_count} jobs that are not ended')