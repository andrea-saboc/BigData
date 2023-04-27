import csv
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
import os
import time
import sys

if __name__ == '__main__':

    HDFS_NAMENODE = "hdfs://namenode:9000"

    spark = SparkSession\
        .builder\
        .appName("HDFSData")\
        .getOrCreate()

    df = spark.read.csv("../home/batch_primeri/traffic_crashes.csv", header=True)
    df.write.csv(HDFS_NAMENODE + "/raw/traffic_crashes.csv", mode="overwrite", header=True)

    df_people = spark.read.csv("../home/batch_primeri/people.csv", header=True)
    df_people.write.csv(HDFS_NAMENODE + "/raw/people.csv", mode="overwrite", header=True)

    df_vehicles = spark.read.csv("../home/batch_primeri/vehicles.csv", header=True)
    df_vehicles.write.csv(HDFS_NAMENODE + "/raw/vehicles.csv", mode="overwrite", header=True)

    df_regions = spark.read.csv("../home/batch_primeri/regions.csv", header=True)
    df_regions.write.csv(HDFS_NAMENODE + "/raw/regions.csv", mode="overwrite", header=True)




