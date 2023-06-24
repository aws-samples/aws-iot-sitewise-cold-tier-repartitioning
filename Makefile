# Configure AWS Credentials 
# Credentials configured using environment variables will take precedence over profile
profile = default

# Python
python_alias = python3

# Configure Glue jobs
job_name_prefix = sitewise-cold-tier-repartitioning
glue_role_arn = <your_glue_role_arn>

export AWS_PROFILE := $(profile)

build:
	$(python_alias) src/generate_globals.py
	$(python_alias) src/build.py

execute:
	$(python_alias) src/job_controller.py $(from) $(to) $(days_per_job) $(job_name_prefix) $(glue_role_arn)

cleanup:
	$(python_alias) src/cleanup_jobs.py

.PHONY: build execute cleanup