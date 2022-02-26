import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_date
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    get spark object and if not exist create new one with preferred configuration
    
    return:
        - spark object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    read songs data and create songs and artists tables
    
    Parameters:
        - spark: a sprak object
        - input_data: raw data path
        - output_data: processed data path
    """
    # get filepath to song data file
    song_data = input_data + 'song_data'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["artist_id", "song_id", "title", "duration", "year"]).dropDuplicates(["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + '/song')

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_latitude", "artist_longitude", "artist_location", "artist_name"]).dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + '/artist')


def process_log_data(spark, input_data, output_data):
    """
    read logs data and create users, time and songpalys tables
    
    Parameters:
        - spark: a sprak object
        - input_data: raw data path
        - output_data: processed data path
    """
    # get filepath to log data file
    log_data = input_data + 'log_data'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df['page'] == 'NextSong')

    # extract columns for users table    
    users_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level']).dropDuplicates(['userId'])
    
    # write users table to parquet files
    users_table.write.parquet(output_data + '/users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0))
    df = df.withColumn('new_ts', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    df = df.withColumn('date', to_date('timestamp'))
    
    # extract columns to create time table
    time_table = df.select(['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "artist_id").parquet(output_data + '/time')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.select(['songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 
                                      'session_id', 'location','user_agent']).dropDuplicates(['songplay_id'])

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "artist_id").parquet(output_data + '/songplay')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
