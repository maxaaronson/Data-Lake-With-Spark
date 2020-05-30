import configparser
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, date_format, dayofmonth, hour, month,
                                   udf, weekofyear, year)

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Creates a Spark Session object for reading and writing data to S3
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def get_date_part(ts, part):
    '''
    Returns the specified part of a date
    Inputs: (int) a timestamp, (str) date part
    '''
    dt = datetime.fromtimestamp(ts / 1000.0)
    if part in ("hour", "day", "month", "year"):
        return getattr(dt, part)
    if part in ("weekday"):
        return dt.weekday()
    if part in ("week"):
        return dt.isocalendar()[1]


def process_song_data(spark, input_data):
    '''
    Read the song data from the S3 bucket and write out the
    songs and artists tables
    Inputs: A Spark session, (str) S3 location
    '''
    # read song data into Spark dataframe
    song_data = spark.read.json(input_data)
    song_data.createOrReplaceTempView("song_data")

    # create songs table
    songs_table = spark.sql('''
        SELECT song_id, title, artist_id, year, duration FROM song_data
        ''')

    # create songs view for join query
    songs_table.createOrReplaceTempView('songs')

    # create artists table
    artists_table = spark.sql('''
        SELECT artist_id, artist_name, artist_location,
            artist_latitude, artist_longitude
        FROM song_data
        ''')

    # write out tables to S3
    artists_table.write.mode('overwrite').parquet(
        config['S3']['DESTINATION_BUCKET'] + 'artists_table.data')

    songs_table.write.mode('overwrite').partitionBy(
        'year', 'artist_id').parquet(
        config['S3']['DESTINATION_BUCKET'] + 'songs_table.data')


def process_log_data(spark, input_data):
    '''
    Read the log data from S3 and write out the user, time
    and songplays tables
    Inputs: A Spark session, (str) S3 location
    '''
    # read log data into Spark dataframe
    user_log = spark.read.json(input_data)
    user_log.createOrReplaceTempView("user_log_table")

    # filter by page = NextSong
    filtered_user_log = spark.sql('''
        SELECT * FROM user_log_table WHERE page = "NextSong"
        ''')
    filtered_user_log.createOrReplaceTempView("filtered_user_log")

    # create users table
    user_table = spark.sql('''
        SELECT userId, firstName, lastName, gender, level
        FROM filtered_user_log
        ''')

    # register get_date_part function with Spark
    spark.udf.register("get_date_part", get_date_part)

    # create time table
    time_table = spark.sql('''SELECT ts,
                            get_date_part(ts, "hour") as hour,
                            get_date_part(ts, "day") as day,
                            get_date_part(ts, "month") as month,
                            get_date_part(ts, "year") as year,
                            get_date_part(ts, "week") as week,
                            get_date_part(ts, "weekday") as weekday
                          FROM filtered_user_log''')

    # extract columns from joined song and
    # log datasets to create songplays table
    songplays_table = spark.sql('''
        SELECT filtered_user_log.ts,
            get_date_part(filtered_user_log.ts, "year") as year,
            get_date_part(filtered_user_log.ts, "month") as month,
            filtered_user_log.userId,
            filtered_user_log.level,
            songs.song_id,
            songs.artist_id,
            filtered_user_log.sessionId,
            filtered_user_log.location,
            filtered_user_log.userAgent
        FROM filtered_user_log
        LEFT JOIN songs
            ON filtered_user_log.song = songs.title
            AND filtered_user_log.length = songs.duration
        WHERE userId IS NOT NULL
        AND filtered_user_log.artist IS NOT NULL
        AND filtered_user_log.song IS NOT NULL
    ''')

    # write tables to S3
    user_table.write.mode('overwrite').parquet(
        config['S3']['DESTINATION_BUCKET'] + 'user_table.data')

    time_table.write.mode('overwrite').partitionBy(
        "year", "month").parquet(
        config['S3']['DESTINATION_BUCKET'] + 'time_table.data')

    songplays_table.write.mode('overwrite').partitionBy(
        "year", "month").parquet(
        config['S3']['DESTINATION_BUCKET'] + 'songplays_table.data')


def main():
    spark = create_spark_session()
    process_song_data(spark, config['S3']['SONG_DATA'])
    process_log_data(spark, config['S3']['LOG_DATA'])

if __name__ == "__main__":
    main()
