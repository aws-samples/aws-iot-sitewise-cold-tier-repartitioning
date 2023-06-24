config = {'timeseries_type': 'ASSOCIATED', 's3': {'cold_tier': {'bucket_name': 'coldtier2', 'data_prefix': 'raw/'}, 'repartitioned': {'bucket_name': 'sitewise-consolidated-tier-1', 'data_prefix': 'consolidated/', 'index_prefix': 'index/'}, 'glue_assets': {'bucket_name': 'sitewise-cold-tier-repartitioning-assets', 'scripts_prefix': 'scripts/'}}}
avro_schema = {'type': 'record', 'name': 'RawDatum', 'namespace': 'amazon.aws.iot.sitewise.raw', 'fields': [{'type': 'string', 'name': 'seriesId'}, {'type': 'long', 'name': 'timeInSeconds'}, {'type': 'long', 'name': 'offsetInNanos'}, {'type': 'string', 'name': 'quality'}, {'type': ['null', 'double'], 'name': 'doubleValue', 'default': None}, {'type': ['null', 'string'], 'name': 'stringValue', 'default': None}, {'type': ['null', 'int'], 'name': 'integerValue', 'default': None}, {'type': ['null', 'boolean'], 'name': 'booleanValue', 'default': None}, {'type': ['null', 'string'], 'name': 'jsonValue', 'default': None}, {'type': ['null', 'long'], 'name': 'recordVersion', 'default': None}]}