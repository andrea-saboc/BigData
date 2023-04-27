#pip install turfpy -- for geolocations

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
from datetime import datetime, timedelta


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

def convert_to_chicago_tz(last_updt):
    dt = datetime.strptime(last_updt, "%Y-%m-%d %H:%M:%S.%f")
    return pytz.timezone('America/Chicago').localize(dt)

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


    convert_to_chicago_tz_udf = udf(convert_to_chicago_tz)


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


    number_per_level=df \
        .withWatermark("timestamp", "15 seconds")\
        .groupBy(window(col("timestamp"), "10 seconds", "8 seconds"), col("congestion_level"))\
        .sum("_length")\
        .withColumn("window", to_json(col("window")))

    number_per_level = number_per_level.filter(col("congestion_level")=="HIGH_CONGESTION")

    write = number_per_level\
       .writeStream\
          .outputMode("complete")\
         .foreachBatch(lambda df, epoch_id: write_df(df, epoch_id, "Length_of_congestion_level")) \
        .start()
    write.awaitTermination()