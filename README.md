# AWS IoT SiteWise Cold storage tier Re-partitioning

## About this Repo
This repo provides code samples to re-partition the AWS IoT SiteWise Cold storage tier data and store it into a destination Amazon S3 bucket. Re-partitioning helps with improving Athena query performance for query patterns encompassing multiple time series. The samples provided downloads time series objects from cold storage tier S3 bucket, merges them into daily partitions, and finally, uploads to destination S3 bucket.

Post initial-run, only new data additions are re-partitioned. Following are a few supported scenarios.

|Change in Asset Modeling | Supported? | 
|----|----|
| No change, all existing assets and properties | Y |
| New properties added to an existing asset | Y |
| New assets added | Y |
| New datastream mapped to an existing property | Y |

## How to use
### 1) Configure
Review and update the configuration in the `config.yml` YAML file.

`timeseries_type` - the type of time series to include ASSOCIATED or DISASSOCIATED. 

`cold_tier_bucket_name` - name of the S3 bucket configured in AWS IoT SiteWise Cold tier settings.

`repartitioned_data_bucket_name` - destination S3 bucket name to store the re-partitioned data

`local_tmp_raw_dir_name` - local directory name for temporarily storing raw downloaded data

`local_tmp_merged_dir_name` - local directory name for temporarily storing merged data

`date_start` - start date in '%Y-%m-%d' format

`date_end` - end date in '%Y-%m-%d' format

### 2) Download raw data from AWS IoT SiteWise Cold-tier S3 bucket

Run `download_cold_tier_data.py` to download existing AVRO data files in the Cold tier for the selected `date_start` and `date_end`. The script also downloads any existing `timeseries.txt` index file from the destination S3 bucket.

`timeseries.txt` - a new-line delimited plain text file that stores the list of all time series ids processed in previous runs.

The script reviews the Cold tier data for each day and skips processing if the data was already re-partitioned previously.

If there's new data, corresponding S3 objects will be downloaded to day-wise local directories

    Reviewing --> year: 2023, month: 1, day: 25
            # of new timeseries detected: 35
            Found data, starting to download
            Downloading S3 objects to local filesystem..
                    ** Download time: 3 secs **

If no new data is found, no further processing happens for the day

    Reviewing --> year: 2023, month: 1, day: 10
            # of timeseries previously processed: 50
            Skip, no new data

### 3) Merge data into daily partitions

Run `repartition_data.py` to merge downloaded files in each day-wise directory.  
| Before | After | 
| -- | -- | -- |
| Multiple `.AVRO` files per day | Single `.AVRO` file per day |
| `timeseries.txt` and optionally `previous_timeseries.txt` per day | Single `timeseries.txt` file per day |

Here is a sample output.

    Started merging AVRO data files and index files for each day
    2023-1-7: ** Merge Time: 0 secs **
    2023-1-8: ** Merge Time: 0 secs **
    2023-1-9: ** Merge Time: 0 secs **

    Script execution successfully completed!!
    

### 4) Upload re-partitioned data to target S3 bucket

Run `upload_repartitioned_data.py` to upload processed data into destination S3 bucket configured in `repartitioned_data_bucket_name`

Here is a sample output.

    Started uploading re-partitioned AVRO data files and index file for each day
    2023-1-7: ** Upload Time: 0 secs **
    2023-1-8: ** Upload Time: 0 secs **
    2023-1-9: ** Upload Time: 0 secs **

    Script execution successfully completed!!

## Important Notes
1. You will incur additional storage costs for the re-partitioned data in Amazon S3
2. Consider automating the execution workflow using [Amazon S3 Event Notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html) and AWS Glue [ETL jobs](https://docs.aws.amazon.com/glue/latest/dg/etl-jobs-section.html)
