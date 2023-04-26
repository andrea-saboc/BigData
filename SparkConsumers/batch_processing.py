import csv
import logging

from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
import os
import time
import sys
from datetime import datetime
from pyspark.sql import functions
from pyspark.sql.functions import col,regexp_extract, split, ceil,count,year,min, max,avg,when,lower,mean,desc, to_timestamp, regexp_replace, trim, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

#1 - number of accidents by years (number divided by NO INJURY and INJURY accidents)
def accidents_by_years(df):
    df = df.select("CRASH_DATE", "CRASH_TYPE")
    df_years = df.withColumn("Year", year(to_timestamp("CRASH_DATE", "MM/dd/yyyy hh:mm:ss a"))).drop("CRASH_DATE")

    accidents_by_years = df_years.groupBy("Year").agg(count("*").alias("Total accidents"),
        count(when(df_years.CRASH_TYPE =='NO INJURY / DRIVE AWAY',1)).alias("No injury/Drive by accidents"),
        count(when(df_years.CRASH_TYPE=='INJURY AND / OR TOW DUE TO CRASH',1)).alias("Injury and/or tow due to crash"))
    accidents_by_years.show()
    write_df(accidents_by_years, "AccidentsByYears")

#2 frequencies of 20 most common car make that caused accidents in which was people from 18-65 years old
def accidents_by_vehicle(df, df_vehicle, df_people):
    crashes_df = df.join(df_vehicle, how="inner", on="CRASH_RECORD_ID").join(df_people, how="inner", on="CRASH_RECORD_ID")
    crashes_df = crashes_df.select("MAKE", "MODEL", "VEHICLE_ID", "AGE")
    window = Window.partitionBy("MAKE").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    df_filtered_age = crashes_df.where(col("AGE").between(18,65))
    df_filtered_age = df_filtered_age.drop("AGE")

    df_filtered_age = df_filtered_age.withColumn("TOTAL_CRASH_MAKE", count("*").over(window))
    df_filtered_age = df_filtered_age.where(col("MAKE")!="UNKNOWN")

    w3 = Window.partitionBy()

    df_filtered_age = df_filtered_age.withColumn("TOTAL", count("*").over(w3))
    df_freq = df_filtered_age.withColumn("TOTAL", count("*").over(w3))

    df_freq=df_freq.withColumn("FREQUENCY", col("TOTAL_CRASH_MAKE")/col("TOTAL"))
    w4 = Window.orderBy(desc("FREQUENCY"))

    df_freq = df_freq.withColumn("RANK", dense_rank().over(w4))
    df_freq.show()
    df_freq = df_freq.groupBy("MAKE", "FREQUENCY", "RANK").agg(count("*").alias("TOTAL_CRASH_MAKE"))
    df_memo = df_freq.where(col("RANK")<20)
    
    write_df(df_memo, "MOST_COMMON_CAR")


#3 accidents by the period of the day, day of the week and age of the driver
def accidents_by_age_of_driver_and_period_of_day(df, df_people):

    df_people = df_people.select("CRASH_RECORD_ID", "AGE", "PERSON_TYPE")
    df_people = df_people.filter(col("AGE").isNotNull())
    df_people = df_people.filter(col("AGE")>0)


    joined_df = df_people.join(df.select("CRASH_RECORD_ID", "CRASH_HOUR"), how="inner", on="CRASH_RECORD_ID")
    joined_df = joined_df.filter(joined_df["PERSON_TYPE"]=="DRIVER")
    joined_df = joined_df.withColumn("AGE_RANGE", when(col("AGE").between(0,25), "<25")
                                     .when(col("AGE").between(25,35), "25-35")
                                    .when(col("AGE").between(36,60), "36-60")
                                    .when(col("AGE").between(60,70), "60-70")
                                    .otherwise("70+"))
    
    joined_df.drop("AGE")

    joined_df = joined_df.withColumn("PERIOD_OF_DAY", when(col("CRASH_HOUR").between(5,9), "morning")
                                                     .when(col("CRASH_HOUR").between(10,13), "late morning")
                                                     .when(col("CRASH_HOUR").between(14,19), "afternoon")
                                                     .otherwise("night time"))

    joined_df = joined_df.drop("CRASH_HOUR")
    grouped_df = joined_df.groupBy("PERIOD_OF_DAY", "AGE_RANGE").agg(count("AGE_RANGE").alias("NUM_ACCIDENTS"))
    pivoted_df = grouped_df.groupBy("PERIOD_OF_DAY").pivot("AGE_RANGE").sum("NUM_ACCIDENTS")

    write_df(pivoted_df, "AGE_PER_DAY")

#4 accidents by part of the day and month
def number_of_accidents_by_part_of_day_and_months(df):
    df = df.select("CRASH_HOUR", "CRASH_MONTH")
    df_hours_ranged = df.withColumn("PERIOD_OF_DAY", when(col("CRASH_HOUR").between(5,9), "morning")
                                                     .when(col("CRASH_HOUR").between(10,13), "late morning")
                                                     .when(col("CRASH_HOUR").between(14,19), "afternoon")
                                                     .otherwise("night time"))

    df_hours_ranged = df_hours_ranged.drop("CRASH_HOUR")
    df_gruoped = df_hours_ranged.groupBy("PERIOD_OF_DAY", "CRASH_MONTH").agg(count("CRASH_MONTH").alias("NUM_ACCIDENTS"))
    #pivot po months
    pivoted_df = df_gruoped.groupBy("CRASH_MONTH").pivot("PERIOD_OF_DAY").sum("NUM_ACCIDENTS").fillna(0)
    pivoted_df = pivoted_df.withColumn("TOTAL", sum(pivoted_df[col] for col in pivoted_df.columns if col!="CRASH_MONTH")).sort(col("CRASH_MONTH").asc())
    pivoted_df= pivoted_df.withColumn("CRASH_MONTH", col("CRASH_MONTH").cast(IntegerType()))
    pivoted_df = pivoted_df.sort(col("CRASH_MONTH").asc())

    write_df(pivoted_df, "NO_ACCIDENTS_PD_M")


#5 accidents by part of the day and day of the week

def number_of_accidents_by_part_of_day_and_day_of_the_week(df):
    df = df.select("CRASH_HOUR", "CRASH_DAY_OF_WEEK")
    df_hours_ranged = df.withColumn("PERIOD_OF_DAY", when(col("CRASH_HOUR").between(5,9), "morning")
                                                     .when(col("CRASH_HOUR").between(10,13), "late morning")
                                                     .when(col("CRASH_HOUR").between(14,19), "afternoon")
                                                     .otherwise("night time"))
    df_hours_ranged = df_hours_ranged.drop("CRASH_HOUR")
    df_gruoped = df_hours_ranged.groupBy("PERIOD_OF_DAY", "CRASH_DAY_OF_WEEK").agg(count("CRASH_DAY_OF_WEEK").alias("NUM_ACCIDENTS"))
    #pivot po months
    pivoted_df = df_gruoped.groupBy("CRASH_DAY_OF_WEEK").pivot("PERIOD_OF_DAY").sum("NUM_ACCIDENTS").fillna(0)
    pivoted_df= pivoted_df.withColumn("TOTAL", sum(pivoted_df[col] for col in pivoted_df.columns if col!="CRASH_DAY_OF_WEEK"))
    pivoted_df= pivoted_df.withColumn("CRASH_DAY_OF_WEEK", col("CRASH_DAY_OF_WEEK").cast(IntegerType()))
    pivoted_df=pivoted_df.sort(col("CRASH_DAY_OF_WEEK").asc())


    write_df(pivoted_df, "NO_ACCIDENTS_PD_DOW")



#6 top 5 causes of fatal accidents in the last year by age of the driver
def cause_of_fatal_accidents_by_age_of_driver_from_last_year(df, df_people):

    last_year = datetime.now().year-1
    df = df.withColumn("Year", year(to_timestamp("CRASH_DATE", "MM/dd/yyyy hh:mm:ss a")))
    df_last_year = df.filter(df["Year"]==last_year).filter(df["INJURIES_FATAL"]>0)
    joined_df = df_people.join(df_last_year.select("CRASH_RECORD_ID", "PRIM_CONTRIBUTORY_CAUSE"), how="inner", on="CRASH_RECORD_ID")

    joined_df = joined_df.withColumn("Age range", when(col("AGE").between(0,20), "<21")
                                    .when(col("AGE").between(21,25), "21-25")
                                    .when(col("AGE").between(26,35), "26-35")
                                    .when(col("AGE").between(36,65), "36-65")
                                    .otherwise("60+"))

    joined_df = joined_df.drop("AGE")

    joined_df = joined_df.groupBy("PRIM_CONTRIBUTORY_CAUSE", "Age range").agg(count("PRIM_CONTRIBUTORY_CAUSE").alias("NUM_ACCIDENTS"))

    joined_df = joined_df.withColumn("PRIM_CONTRIBUTORY_CAUSE", trim(regexp_replace("PRIM_CONTRIBUTORY_CAUSE", "[.]", "_")))



    #limit 5 most commot for each age range

    top_5_reasons = joined_df.groupBy("Age range").agg(count("Age range").alias("total_accidents"))

    ranked_df = joined_df.withColumn("rank", dense_rank().over(
        Window.partitionBy("Age range").orderBy(col("NUM_ACCIDENTS").desc())
    )).filter(col("rank")<=5)

    pivoted_df = joined_df.groupBy("Age range").pivot("PRIM_CONTRIBUTORY_CAUSE").sum("NUM_ACCIDENTS")
    pivoted_df = pivoted_df.drop("UNABLE TO DETERMINE").fillna(0)


    #Calculating percentages
    causes_cols =[c for c in pivoted_df.columns if c != "Age range"]
    pivoted_df = pivoted_df.withColumn("TOTAL", sum([col(c) for c in causes_cols]))

    for col_name in pivoted_df.columns:
        if col_name not in ["Age range", "TOTAL"]:
            number_accidents_cause = col(col_name)
            percentage_accidents_cause = (number_accidents_cause*100)/col("TOTAL")
            pivoted_df = pivoted_df.withColumn(col_name, percentage_accidents_cause)

    pivoted_df = pivoted_df.drop("TOTAL")

    top5_cause_list = ranked_df.select("PRIM_CONTRIBUTORY_CAUSE").distinct().rdd.flatMap(lambda x:x).collect()
    top5_cols = [c for c in pivoted_df.columns if c in top5_cause_list]
    other_cols = [c for c in pivoted_df.columns if c not in top5_cause_list and c!="Age range"]

    pivoted_df = pivoted_df.withColumn("Others", sum(col(c) for c in other_cols))
    pivoted_df = pivoted_df.drop(*other_cols)
    pivoted_df.show(n=5)
    write_df(pivoted_df, "Accidents_by_age_and_cause")


# 7 crash type of the accidents
def number_of_type_of_accidents(df):
    df_types = df.select(col("CRASH_TYPE").alias("type"))
    df_types = df_types.filter(col('type').isNotNull())
    df_types.show(n=10)


    df_grouped = df_types.groupBy("type").agg(count("*").alias("count"))
    df_grouped.show()



    write_df(df_grouped, "types")

#how many of crashes involve injuries or fatslities

def write_df(dataframe,tablename):
    PSQL_SERVERNAME= "postgres"
    PSQL_PORTNUMBER = 5432
    PSQL_DBNAME = "postgres"
    PSQL_USERNAME = "postgres"
    PSQL_PASSWORD = "postgres"
    URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"

    try:
        dataframe.write.format("jdbc").options(
            url=URL,
            driver="org.postgresql.Driver",
            user=PSQL_USERNAME,
            password=PSQL_PASSWORD,
            dbtable=tablename
        ).mode("overwrite").save()
    except Exception as e:
        print("Error saving DataFrame to database:", e)


if __name__ == '__main__':

    HDFS_NAMENODE = "hdfs://namenode:9000"

    logging.getLogger("py4j").setLevel(logging.WARN)


    spark = SparkSession\
        .builder\
        .appName("BatchAnalysis") \
        .config("spark.driver.extraJavaOptions", "-Dlog4jspark.root.logger=WARN,console")\
        .getOrCreate()


    df = spark.read.csv(HDFS_NAMENODE + "/transformation/traffic_crashes.csv", header=True)
    df_people = spark.read.csv(HDFS_NAMENODE + "/transformation/people.csv", header=True)
    df_vehicle = spark.read.csv(HDFS_NAMENODE + "/transformation/vehicles.csv", header=True)


    print("number of enti", df.count())

    #number_of_type_of_accidents(df)
    #accidents_by_years(df)
    #cause_of_fatal_accidents_by_age_of_driver_from_last_year(df, df_people)
    #number_of_accidents_by_part_of_day_and_months(df)
    #number_of_accidents_by_part_of_day_and_day_of_the_week(df)
    #accidents_by_age_of_driver_and_period_of_day(df, df_people)
    accidents_by_vehicle(df, df_vehicle, df_people)