# Project description: A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. so In this project, we pulled data from the S3 bucket, which were events data and songs data are, then perform our ETL using Spark, and load the data back to s3.

# Database design: We implemented a star schema, in which the `songplay` is the fact table, and `users`, `song`, `artist`, `time` are the dimension tables.

# Project Repository files:
    1 - dl.cfg: a config file contains AWS credentials
    2 - etl.py: contains our ETL proccess

# How To Run the Project:
    1 - RUN etl.py