import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import boto3
from botocore.exceptions import ClientError
import logging

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_bucket(bucket_name, region=None, acl="private"):
    """
    Create an S3 bucket in a specified region
    
    Args:
        bucket_name: Bucket to create
        region: String region to create bucket in, e.g., 'us-east-1'
        acl:access control list to apply to the bucket. 'public-read'
            makes sure everything posted is publicly readable
    Return:
        bool : that refer to succeeded or failed bucket creation
    """
    
    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            s3_client.create_bucket(
                Bucket=bucket_name,
                ACL=acl
            )
    except ClientError as e:
        logging.error(e)
        return False
    return True

def create_spark_session():
    """
    Create an spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Transform raw song data from S3 into analytics tables on S3
    
    This function reads in song data in JSON format from S3, defines the schema
    of songs and artists analytics tables, processes the raw data into
    those tables then writes the tables into partitioned parquet files on
    S3.
    
    Args:
        spark: a Spark session
        input_data: an S3 bucket to read song data in from
        output_data: an S3 bucket to write analytics tables to
    """
    
    # get filepath to song data file
    song_data = f"{input_data}song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration']).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year','artist_id']).parquet("{}/songs.parquet".format(output_data), mode="overwrite")

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').distinct()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(path=output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    """
    Transform raw log data from S3 into analytics tables on S3
    
    This function reads in log data in JSON format from S3, defines the schema
    of logs and artists analytics tables, processes the raw data into
    those tables then writes the tables into partitioned parquet files on
    S3.
    
    Args:
        spark: a Spark session
        input_data: an S3 bucket to read log data in from
        output_data: an S3 bucket to write analytics tables to
    """
    # get filepath to log data file
    log_data = f"{input_data}log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data) 
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').distinct()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(path=output_data + 'users')
    
    # extract columns to create time table
    df.createOrReplaceTempView("logs") 
    time_table = spark.sql('select t.start_time, \
           hour(t.start_time) as hour, \
           day(t.start_time) as day, \
           weekofyear(t.start_time) as week, \
           month(t.start_time) as month, \
           year(t.start_time) as year, \
           dayofweek(t.start_time) as weekday\
           from \
           (select from_unixtime(ts/1000) as start_time from logs group by start_time) t')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year','month']).parquet("{}/times.parquet".format(output_data), mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet("{}/songs.parquet".format(output_data))

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView('songs')
    songplays_table = spark.sql('select \
          monotonically_increasing_id() as songplay_id,  \
          from_unixtime(l.ts/1000) as start_time, \
          userId as user_id,\
          l.level,\
          s.song_id,\
          s.artist_id,\
          l.sessionId as session_id,\
          l.location,\
          l.userAgent as user_agent\
          from \
          logs l \
          left join songs s on l.song = s.title') 
    
    songplays_table = songplays_table.withColumn('year', year(songplays_table.start_time)).withColumn('month', month(songplays_table.start_time))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year','month']).parquet("{}/songplays.parquet".format(output_data), mode="overwrite")


def main():
    """
    main method to run ETL pipeline
    """
    spark = create_spark_session()
    input_data = config['S3']['SOURCE_S3_BUCKET']
    output_data = config['S3']['DEST_S3_BUCKET']
    create_bucket(output_data.split('//')[1][:-1],region="us-east-1",acl="public-read")
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
