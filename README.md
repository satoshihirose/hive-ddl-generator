# hive-ddl-generator
This script generates DDL template of hive with guessing column types from a file.
## Dependency
```
pip install messytables smart_open
```
## How to use
Prepare data and save it on S3
```
$ cat test.csv
intcolumn,boolcolumn,doublecolumn,datecolumn,stringcolumn
10,true,0.1,'2016/08/09','abcs'
```
then, execute script!
```
$ python generate_ddl.py s3://[mybucketname]/athena/data/test.csv
file path:  s3://[mybucketname]/athena/data/test.csv
delimiter:  ,
date_format:  %Y/%m/%d
guessed types:  [Integer, Bool, Decimal, Date(%Y/%m/%d), String]
----------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS default.test.csv (
  `a0` int,
  `a1` boolean,
  `a2` double,
  `a3` date,
  `a4` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "'",
    "escapeChar"    = "\\"
) LOCATION 's3://[mybucketname]/athena/data'
TBLPROPERTIES ('has_encrypted_data'='false');
----------------------------------------------------------
```
## Supported formats
CSV
### Supported types
string, date, int, double, boolean
## TODO
support for other file formats such as parquet, json, orc, avro etc...
