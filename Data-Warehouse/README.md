# Project description: A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.so In this project, we pulled data from the S3 bucket, which were events data and songs data are, then we connected to AWS RedShift, and created our fact and dimension tables.

# Database design: We have a single database that contains 5 tables, we implemented a star schema, in which the `songplay` is the fact table, and `users`, `song`, `artist`, `time` is the dimension tables.

# ETL Process: We pulled all the events data and songs data from the S3 bucket into staging tables, then insert them into our fact and dimension tables on RedShift.

# Project Repository files:
* sql_queries.py: Contain all the necessary queries to drop, create, copy, and insert into table.
* create_tables.py: It first establishes a connection to the database, then drops all the tables, and finally recreates all the tables.
* etl.py: Perform the actual data processing and data insertion into the tables.
* dwh.cfg: Contain the RedShift database info, credentials, and S3 data paths.
# How To Run the Project:
    1- RUN create_tables.py
    2- RUN etl.py