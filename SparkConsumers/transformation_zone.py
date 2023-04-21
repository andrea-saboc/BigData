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

    df = spark.read.csv(HDFS_NAMENODE + "/raw/traffic_crashes.csv", header=True)

    print("Number of rows ", df.count())


    df_null = df = df.withColumn("null_count",
                                 reduce(lambda acc, c: acc + c, [when(isnull(c), 1).otherwise(0) for c in df.columns]))
    #df = df.filter(df_null.null_count <= 5)
    df = df.drop("null_count")
    df = df.drop("RD_NO")


    df.write.csv(HDFS_NAMENODE + "/transformation/traffic_crashes.csv", mode="overwrite",
                                 header=True)
    print("Names of the columns ", df.columns)
    print("Number of rows filtered", df.count())

    df.show(n=5)
