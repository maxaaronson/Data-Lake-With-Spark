# Data Lake With Spark

### Purpose
This project will read `song` and `user_log` data from an S3 bucket, process that data into a star schema using `Spark`, and then write the data out to a different S3 bucket in `parquet` format.  The schema contains a fact table, 'songplays', and four dimension tables: songs, artists, users and time.

### Getting Started
The project can be run from whatever folder the Python script is located in

### Requirements
- `pyspark.sql` must be installed 
- An AWS IAM user with S3 read and write permissions to access the buckets

### To Run
From the command line, use `python etl.py`

### Testing
Ensure the fact and dimension tables have been written to the destination bucket on S3.