# Project description: In this project, we create a database with all the necessary tables to load and store the data files.
# Database design: We have a single database that contains 5 tables, we implemented a star schema, in which the `songplay_table` is the fact table, and `user`, `song`, `artist`, `time` is the dimension tables.
# ETL Process: We loads all the JSON files in both songs and log directories into dataframes, then we iterated over the dataframes and insert the data into its corresponding fact or dimension tables.
# Project Repository files:
* data: Contain both the log data and the song data.
* sql_queries.py: Contain all the necessary queries to drop, create and insert into table.
* create_tables.py: It first establishes a connection to the database, then drops all the tables, and finally recreates all the tables.
* etl.py: Perform the actual data processing and data insertion into the tables.
* etl.ipynb & test.ipynb: Both are helping notebooks to visualse and test the ETL process.
# How To Run the Project:
    1- RUN create_tables.py
    2- RUN etl.py