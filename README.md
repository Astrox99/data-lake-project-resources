# Data Lake project

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

My role is to building an ETL pipeline that extracts their data from S3, process them using Spark, and load the data back into S3 as a set of dimentional tables. 

There are two datasets that need to be extracted:
    - **Song dataset**: It contains metadata about a song and the artist of that song.
    - **Log dataset**: It contains activity logs of users. The dataset is partitioned by year and month.
    
They both are in .JSON files.

##### A Star schehma is required in this project.

###### Fact table:
    1. songplays - Records in log data associated with song plays

###### Dimension Tables
    2. users - Users in the app
    3. songs - Songs in music database
    4. artists - Artists in music database
    5. time - timestamps of records in songplays broken down into specific units

##### Project Files:
    - etl.py: Reads data from S3, processes that data using Spark, and writes them back to S3
    - dl.cfg: Contains AWS credentials
    - README.MD: Provides discussion on your process and decisions
    - data: The subset data of song_data and log_data

##### Installation requirements:
To run the ETL process locally, log_data and song_data files are required to be unzipped before extraction.

```bash
$ unzip data/log_data.zip -d data
$ unzip data/song_data.zip -d data
$ mkdir output
```


##### Build ETL pipeline
    1. Set AWS_ACCESS_KEY_ID and AWS_ACCESS_KEY_ID in dl.cfg file.
    2. Create S3 bucket in AWS extranet and set the URL to the output_data in the main() function.
    3. Run etl.py 
        ```bash
        python etl.py
        ```    
        This file will do the following tasks:
            - Read data files from S3.
            - Extract columns to create a table.
            - Write a table to parquet files.
            - Load it back to S3
    4. Delete the output S3 when fisnished to save cost.

