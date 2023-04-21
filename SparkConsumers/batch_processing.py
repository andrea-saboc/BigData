import csv
import logging

from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
import os
import time
import sys
from pyspark.sql import functions
from pyspark.sql.functions import col,regexp_extract, split, ceil,count,year,min, max,avg,when,lower,mean,desc
from pyspark.sql.window import Window



def number_of_type_of_accidents(df):
    df_types = df.select(col("CRASH_TYPE").alias("type"))
    df_types = df_types.filter(col('type').isNotNull())
    df_types.show(n=10)


    df_grouped = df_types.groupBy("type").agg(count("*").alias("count"))
    df_grouped.show()



    write_df(df_grouped, "types")

def write_df(dataframe,tablename):
    PSQL_SERVERNAME= "postgres"
    PSQL_PORTNUMBER = 5432
    PSQL_DBNAME = "postgres"
    PSQL_USERNAME = "postgres"
    PSQL_PASSWORD = "postgres"
    URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"

    dataframe.write.format("jdbc").options(
        url=URL,
        driver="org.postgresql.Driver",
        user=PSQL_USERNAME,
        password=PSQL_PASSWORD,
        dbtable=tablename
    ).mode("overwrite").save()

"""
def rating_by_genre(movie_df):
    df_exploded = movie_df.withColumn("genre", functions.explode(movie_df.genre))
    avg_df = df_exploded.groupBy("genre").agg(avg("rating"))
    avg_df.show()
    write_df(avg_df,"average_ratings")


def ratings_by_clients_and_proffesionals(movie_df,ratings_df):
    ratings_df = ratings_df.withColumnRenamed("rating", "professional_rating").withColumn("professional_rating", col("professional_rating").cast("double"))
    movie_df = movie_df.withColumnRenamed("rating", "client_rating").withColumn("client_rating", col("client_rating").cast("double"))

    avg_client_ratings_df = ratings_df.groupBy("movie_id").avg("professional_rating")

    avg_client_ratings_df.show()

    joined_by_rating = movie_df.join(avg_client_ratings_df,"movie_id")
    joined_and_selected = joined_by_rating.select("movie_id", "avg(professional_rating)", "client_rating")
    write_df(joined_and_selected,"client_profesional_ratings")

def ratings_by_duration(df):
    df = df.withColumn("hours", split(col("duration"), "h")[0])
    df = df.withColumn("minutes_hours", split(col("duration"), "min")[0])
    df = df.withColumn("minutes", split(col("minutes_hours"), " ")[1])
    # Convert hours and minutes to minutes
    df = df.withColumn("duration_minutes", col("hours") * 60 + col("minutes"))

    # Round up to the nearest 15 minutes
    df = df.withColumn("duration_group", ceil(col("duration_minutes") / 15.0) * 15)
    df = df.groupBy("duration_group").mean("rating").sort("duration_group", ascending=False)

    # Select the desired columns and show the results
    df = df.select("duration_group", "avg(rating)")
    df_final = df.dropna(how="any", subset=None)
    df_final.show()
    write_df(df_final,"duration_ratings")

def comment_count_ratings(df):
    df = df.groupBy("movie_id")
    df = df.agg(count("movie_id").alias("comment_count"), avg("rating").alias("avg_rating"))
    df = df.orderBy(df["comment_count"].desc())
    df = df.select("movie_id", "avg_rating","comment_count")
    df.show()
    write_df(df,"comments_by_movie")

def best_and_worst_reviews(df):
    windowSpec = Window.partitionBy("movie_id").orderBy("rating")
    df = df.withColumn("worst_rating", min("rating").over(windowSpec))
    df = df.withColumn("best_rating", max("rating").over(windowSpec))
    df = df.select("movie_id", "worst_rating","best_rating")
    df = df.orderBy(df["best_rating"].desc())
    write_df(df,"best_and_worst")

def spoiler_percentage(df):
    df=df.select(mean(df["is_spoiler"]))
    write_df(df,"spoiler_percentage")

def ratings_by_years(df):
    df = df.withColumn("year", year("release_date"))
    df = df.groupBy("year")

    # Compute the average rating
    df = df.avg("rating")

    # Sort the DataFrame by the year column in ascending order
    df = df.orderBy(df["year"].asc())
    write_df(df,"ratings_by_year")

def average_rating_spoilers(df):
    df = df.groupBy("is_spoiler").agg(mean("rating").alias("avg_rating"))
    write_df(df,"average_rating_spoilers")

def user_activity(df):
    windowSpec = Window.partitionBy("user_id")

    # Count the number of times each user appears in the dataframe
    df = df.withColumn("user_count", count("user_id").over(windowSpec))

    # Display the results
    df= df.select("user_id", "user_count")
    df = df.orderBy(desc("user_count"))
    write_df(df,"user_activity")


def write_df(dataframe,tablename):
    PSQL_SERVERNAME= "postgres"
    PSQL_PORTNUMBER = 5432
    PSQL_DBNAME = "postgres"
    PSQL_USERNAME = "postgres"
    PSQL_PASSWORD = "postgres"
    URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"

    dataframe.write.format("jdbc").options(
        url=URL,
        driver="org.postgresql.Driver",
        user=PSQL_USERNAME,
        password=PSQL_PASSWORD,
        dbtable=tablename
    ).mode("overwrite").save()
    """




if __name__ == '__main__':

    HDFS_NAMENODE = "hdfs://namenode:9000"

    logging.getLogger("py4j").setLevel(logging.WARN)


    spark = SparkSession\
        .builder\
        .appName("BatchAnalysis") \
        .config("spark.driver.extraJavaOptions", "-Dlog4jspark.root.logger=WARN,console")\
        .getOrCreate()


    df = spark.read.csv(HDFS_NAMENODE + "/transformation/traffic_crashes.csv", header=True)

    print("number of enti", df.count())

    number_of_type_of_accidents(df)
    """
    spoiler_percentage(df)
    average_rating_spoilers(df)
    user_activity(df)
    best_and_worst_reviews(df)

    ratings_by_years(movie_df)

    comment_count_ratings(df)

    ratings_by_duration(movie_df)

    rating_by_genre(movie_df)

    ratings_by_clients_and_proffesionals(movie_df,df)
    """