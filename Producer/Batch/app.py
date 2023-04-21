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




