"""
import importlib

try: 
    importlib.import_module('turfpy')
except ImportError:
    import subprocess
    import sys
    # implement pip as a subprocess
    # ref: https://www.activestate.com/resources/quick-reads/how-to-install-python-packages-using-a-script/
    subprocess.check_call(['pip', 'install', 'turfpy']) 
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
from datetime import datetime, timedelta
from pyspark import SparkFiles
import tempfile


#import turfpy
#from turfpy.measurement import boolean_point_in_polygon
#from geojson import Point, MultiPolygon, Feature
#from ipyleaflet import Map, GeoJSON, LayersControl


import os



scema = StructType() \
  .add("segmentid", StringType()) \
  .add("street", StringType()) \
  .add("_fromst", StringType()) \
  .add("_tost", StringType()) \
  .add("_length", StringType()) \
  .add("_strheading", StringType()) \
  .add("_comments", StringType()) \
  .add("start_lon", StringType()) \
  .add("_lif_lat", StringType()) \
  .add("_lit_lon", StringType()) \
  .add("_lit_lat", StringType()) \
  .add("_traffic", StringType()) \
  .add("_last_updt", StringType())

def write_df(df, epoch_id,tablename ):
    print(f"epoch id: {epoch_id}")
    PSQL_SERVERNAME= "postgres"
    PSQL_PORTNUMBER = 5432
    PSQL_DBNAME = "postgres"
    PSQL_USERNAME = "postgres"
    PSQL_PASSWORD = "postgres"
    URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"
    try:
        df.write.format("jdbc").options(
        url=URL,
        driver="org.postgresql.Driver",
        user=PSQL_USERNAME,
        password=PSQL_PASSWORD,
        dbtable=tablename
        ).mode("overwrite").save()
    except Exception as e:
        print(f"Error saving DataFrame to database:{e}")

def clean_data(df):
    df \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", "processed-data") \
    .start()


def convert_to_chicago_tz(last_updt):
    dt = datetime.strptime(last_updt, "%Y-%m-%d %H:%M:%S.%f")
    return pytz.timezone('America/Chicago').localize(dt)


def point_in_the_region(lon, lat, polygon):
    point = Feature(geometry=Point([lon, lat]))
    geometry=MultiPolygon(polygon)
    val = boolean_point_in_polygon(point, polygon)
    print(f"val = {val}")
    return val


def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)



if __name__ == '__main__':

    HDFS_NAMENODE = "hdfs://namenode:9000"
    TOPIC = "chicago-traffic"
    KAFKA_BROKER = "kafka1:19092"

    spark = SparkSession\
        .builder\
        .appName("StreamingProcessing")\
        .getOrCreate()
    quiet_logs(spark)

    spark.sparkContext.addPyFile("../home/consumers/turfpy-0.0.7.tar.gz")
    spark.sparkContext.addPyFile("../home/consumers/shapely-2.0.1.tar.gz")
    spark.sparkContext.addPyFile("../home/consumers/numpy-1.24.3.tar.gz")
    spark.sparkContext.addPyFile("../home/consumers/scipy-1.10.1.tar.gz")


    from geojson import Point, MultiPolygon, Feature
    from turfpy.measurement import boolean_point_in_polygon
    from geojson import Point, MultiPolygon, Feature



    convert_to_chicago_tz_udf = udf(convert_to_chicago_tz)
    point_in_the_region_udf = udf(point_in_the_region)

    df_regions = spark.read.csv(HDFS_NAMENODE + "/transformation/regions.csv", header=True)



    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC) \
        .load()\

        
    df= df.withColumn("value", col("value").cast("string"))\
        .withColumn("parsed_value", from_json(col("value"), scema))

    df = df\
        .select(
            col("timestamp"),
            col("key").cast("string"),
            col("parsed_value.*"),
        )\
        .withColumn("_traffic", col("_traffic").cast(IntegerType()))\
        .withColumn("_lif_lat", col("_lif_lat").cast(DoubleType()))\
        .withColumn("_lit_lon", col("_lit_lon").cast(DoubleType()))\
        .withColumn("_lit_lat", col("_lit_lat").cast(DoubleType()))\
        .withColumn("start_lon", col("start_lon").cast(DoubleType()))\
        .withColumn("_length", col("_length").cast(DoubleType()))\
        .withColumn("_last_updt",  convert_to_chicago_tz_udf("_last_updt"))\
        .withColumn("congestion_level",
                when(col("_traffic")== -1, "UNKNOWN") \
                .when(col("_traffic")== 0, "NO_TRAFFIC") \
                .when(col("_traffic").between(1,10), "HIGH_CONGESTION")\
                .when(col("_traffic").between(11,15), "MODERATE_CONGESTION")\
                .when(col("_traffic").between(16,25), "MILD_CONGESTION")\
                .when(col("_traffic").between(26,35), "BASIC_UNBLOCKED")\
                .when(col("_traffic")>35, "UNBLOCKED")\
                .otherwise("NO_VALUE"))\
        .drop("_traffic")

    df_joined = df.join(df_regions,  how="inner")
    df_joined = df_joined.filter(point_in_the_region_udf("start_lon","_lif_lat", "the_geom").cast('boolean'))

    df_joined = df_joined.select("timestamp", "congestion_level", "REGION_NAM")


    df_with_regions=df_joined \
        .withWatermark("timestamp", "15 seconds")\
        .groupBy(window(col("timestamp"), "10 seconds", "8 seconds"), col("congestion_level"), col("REGION_NAM"))\
        .count()\
        .orderBy(desc("congestion_level"))\
        .withColumn("window", to_json(col("window")))

    df_with_regions = df_with_regions.filter(col("congestion_level")!="UNKNOWN")

    #df_with_regions = df_with_regions.groupBy("REGION_NAM", "window", "congestion_level").agg(sum("count").alias("total_count"))




    write = df_with_regions\
       .writeStream\
          .outputMode("complete")\
         .foreachBatch(lambda df, epoch_id: write_df(df, epoch_id, "Congestion_level_number_per_region")) \
        .start()
    write.awaitTermination()