import csv
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
import os
import time
import sys
from pyspark.sql import functions
from pyspark.sql.functions import *
from functools import reduce

if __name__ == '__main__':
    HDFS_NAMENODE = "hdfs://namenode:9000"

    spark = SparkSession \
        .builder \
        .appName("TransformationZone") \
        .getOrCreate()

    #df = spark.read.csv(HDFS_NAMENODE + "/raw/traffic_crashes.csv", header=True)
    #df_people = spark.read.csv(HDFS_NAMENODE + "/raw/people.csv", header=True)
    #df_vehicles = spark.read.csv(HDFS_NAMENODE + "/raw/vehicles.csv", header=True)
    df_regions = spark.read.csv(HDFS_NAMENODE + "/raw/regions.csv", header=True)


    #df_null = df = df.withColumn("null_count",
    #                             reduce(lambda acc, c: acc + c, [when(isnull(c), 1).otherwise(0) for c in df.columns]))
    #df = df.filter(df_null.null_count <= 5)
    #df = df.drop("null_count")
    #df = df.drop("RD_NO")
    #df.write.csv(HDFS_NAMENODE + "/transformation/traffic_crashes.csv", mode="overwrite", header=True)


    #filter out those people who have no matching values in df
    #filtered_df_people = df_people.join(df.select(col("CRASH_RECORD_ID")).distinct(), on="CRASH_RECORD_ID", how="inner")

    #filtered_df_people1 = filtered_df_people.select("PERSON_ID", "PERSON_TYPE", "CRASH_RECORD_ID", "SEX", "AGE", "PHYSICAL_CONDITION", "CELL_PHONE_USE")
    #df.write.csv(HDFS_NAMENODE + "/transformation/traffic_crashes.csv", mode="overwrite", header=True)
    #filtered_df_people1.write.csv(HDFS_NAMENODE + "/transformation/people.csv", mode="overwrite", header=True)

    #filtered_df_vehicles = df_vehicles.join(df.select(col("CRASH_RECORD_ID")).distinct(), on="CRASH_RECORD_ID", how="inner")
    #filtered_df_vehicles = filtered_df_vehicles.select("CRASH_RECORD_ID", "MAKE", "MODEL", "VEHICLE_ID")
    #filtered_df_vehicles = filtered_df_vehicles.filter((filtered_df_vehicles['MAKE'].isNotNull()) & (filtered_df_vehicles["MAKE"].isNotNull()))
    #filtered_df_vehicles.write.csv(HDFS_NAMENODE+"/transformation/vehicles.csv", mode="overwrite", header=True)

    #print("Number of rows filtered for traffic crashes", df.count())
    #print("Number of rows filtered for peopl ", filtered_df_people1.count())
    #print("People count before filter:", df_people.count())

    df_regions.write.csv(HDFS_NAMENODE +"/transformation/regions.csv", mode="overwrite", header=True)




