# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import json
import yaml
import helpers.common as common_helper

src_dir = os.path.abspath(os.path.dirname(__file__))
root_dir = os.path.abspath(os.path.dirname(src_dir))

globals_path = f'{src_dir}/helpers/globals.py'

# Open an empty globals.py file
if os.path.exists(globals_path): os.remove(globals_path)
file = open(globals_path, 'w')
    
# Write config
with open(f'{root_dir}/config.yml', 'r') as config_file:
    config = yaml.safe_load(config_file)
common_helper.validate_config_inputs(config)

file.write('config = ')
file.write(repr(config))
file.write('\n')

# Write avro schema
with open(f'{root_dir}/avro_schema.json', 'r') as avro_schema_file:
    avro_schema = json.load(avro_schema_file)
file.write('avro_schema = ')
file.write(repr(avro_schema))

file.close()