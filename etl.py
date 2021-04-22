import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types  import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():

    """
        Desctiption: Create Spark session to work with DataFrame. 
        Setup configuration to Hadoop-AWS
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
        Description: Load data from Song Dataset JSON files in S3, extract it into a DataFrame,
        then write DataFrame as parquet files back to S3
    """
    song_data = input_data + 'song_data/*/*/*/*.json'

    songSchema = R([
        Fld("num_songs", Int()),
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("duration", Dbl()),
        Fld("year", Int()),
    ])
    
    df = spark.read.json(song_data, schema=songSchema)

    song_cols = ["song_id", "title", "artist_id", "year", "duration"]    
    songs_table = df.select(song_cols).dropDuplicates()    

    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs/", mode = "overwrite")

    artist_cols = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artists_table = df.select(artist_cols).dropDuplicates()

    artists_table.write.parquet(output_data + "artists/", mode = "overwrite")


def process_log_data(spark, input_data, output_data):
    
    """
    Description: Load data from Log Dataset JSON files in S3, extract it into a DataFrame,
    then write DataFrame as parquet files back to S3

    """
    log_data = input_data + 'log_data/*.json'

    df = spark.read.json(log_data)
    
    df = df.filter(df.page == "NextSong")


    user_cols = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df.selectExpr(user_cols).dropDuplicates()
    
    users_table.write.parquet(output_data + "users/", mode = "overwrite")

    # fromtimestamp function is required to be process in seconds hence, it is divided by 1000
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).isoformat()) 
    df = df.withColumn("start_time", get_timestamp("ts").cast(TimestampType()))
    
    time_table = df.select("start_time") \
        .withColumn("hour", F.hour("start_time")) \
        .withColumn("day", F.dayofmonth("start_time")) \
        .withColumn("week", F.weekofyear("start_time")) \
        .withColumn("month", F.month("start_time")) \
        .withColumn("year", F.year("start_time")) \
        .withColumn("weekday", F.dayofweek("start_time"))
    
    time_table.write.partitionBy("year", "month").parquet(output_data + "time/", mode = "overwrite")

    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    df = df.orderBy("ts")
    df = df.withColumn("songplay_id", F.monotonically_increasing_id())

    song_df.createOrReplaceTempView("staging_songs")
    df.createOrReplaceTempView("staging_events")
   
    songplays_table = spark.sql("""
        SELECT
            se.songplay_id,
            se.start_time,
            se.userId as user_id,
            se.level,
            ss.song_id,
            se.sessionId as session_id,
            ss.artist_id,
            se.location,
            se.userAgent as user_agent,
            YEAR(se.start_time) as year,
            MONTH(se.start_time) as month
        FROM staging_events se
        LEFT JOIN staging_songs ss
        ON (se.song = ss.title 
        AND se.artist = ss.artist_name) 
    """)

    songplays_table.write.partitionBy("year", "month").parquet(output_data + "songplays/", mode="overwrite")
    
 
def main():

    """
        Description: Call a function to create a Spark sessionm, setup directories for input and output data
    
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-output-storage"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
