use role accountadmin;
use warehouse compute_wh;

-- create file format
create or replace file format csv_file_format
    type = csv
    skip_header = 1
    field_optionally_enclosed_by = '"'
    trim_space = true
    error_on_column_count_mismatch = false;

-- create storage integration
create or replace storage integration s3_int
    type = external_stage
    storage_provider = 's3'
    storage_aws_role_arn = 'arn:aws:iam::137386360306:role/snowflake-s3-access'
    enabled = true
    storage_allowed_locations = ('s3://logistics-details/');

-- desc storage integration s3_int

-- create stage
create or replace stage s3_ex_stage
    storage_integration = s3_int
    url = 's3://logistics-details/rawData/'
    file_format = csv_file_format;