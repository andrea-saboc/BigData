from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from pyspark.sql.window import Window
from pyspark.sql.functions import count, sum, col, row_number, rank, dense_rank

# create a SparkSession
spark = SparkSession.builder.appName("example").getOrCreate()

# define the schema for the DataFrame
schema = StructType([
    StructField("CRASH_ID", IntegerType(), True),
    StructField("DATE", StringType(), True),
    StructField("VEHICLE_MAKE", StringType(), True),
    StructField("INJURIES_TOTAL", IntegerType(), True),
    StructField("CRASH_HOUR", IntegerType(), True)
])

# create the DataFrame
data = [(1, "2021-01-01", "Honda", 0, 10),
        (2, "2021-01-01", "Toyota", 2, 11),
        (3, "2021-01-02", "Honda", 1, 9),
        (4, "2021-01-02", "Ford", 3, 14),
        (5, "2021-01-03", "Honda", 0, 18),
        (6, "2021-01-03", "Toyota", 1, 21),
        (7, "2021-01-04", "Honda", 2, 12),
        (8, "2021-01-04", "Toyota", 0, 13),
        (9, "2021-01-05", "Honda", 1, 16),
        (10, "2021-01-05", "Ford", 0, 8),
        (11, "2021-01-06", "Toyota", 3, 19),
        (12, "2021-01-06", "Honda", 0, 20),
        (13, "2021-01-07", "Ford", 1, 22),
        (14, "2021-01-07", "Honda", 0, 17),
        (15, "2021-01-08", "Toyota", 2, 15),
        (16, "2021-01-08", "Honda", 1, 23),
        (17, "2021-01-09", "Ford", 0, 6),
        (18, "2021-01-09", "Toyota", 1, 7),
        (19, "2021-01-10", "Honda", 2, 12),
        (20, "2021-01-10", "Toyota", 0, 13),
        (21, "2021-01-11", "Honda", 3, 14),
        (22, "2021-01-11", "Ford", 0, 15),
        (23, "2021-01-12", "Toyota", 1, 16),
        (24, "2021-01-12", "Honda", 2, 17),
        (25, "2021-01-13", "Ford", 0, 10),
        (26, "2021-01-13", "Honda", 1, 11),
        (27, "2021-01-14", "Toyota", 2, 12),
        (28, "2021-01-14", "Honda", 0, 13),
        (29, "2021-01-15", "Ford", 1, 15)]


df = spark.createDataFrame(data, schema)

window = Window.partitionBy('VEHICLE_MAKE').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)



# Calculate the desired statistics for each vehicle make
#result = accidents_df \
#  .select(
#    'vehicle_make',
#    count('*').alias('total_crashes'),
#    count('*').over(window).alias('crashes_by_make'),
#    sum(count('*')).over(Window.orderBy('*')).alias('total_count'),
#    (count('*') / sum(count('*')).over(window)).alias('frequency')
#  )
print("df1 show")
df1 = df.select('VEHICLE_MAKE', count('*').over(window).alias("CRASHESBYMAKE"))
df1.show()

w2 = Window.orderBy("VEHICLE_MAKE")
w3 = Window.partitionBy()

df1 = df1.withColumn("row_num", dense_rank().over(w2))
df1=df1.withColumn("TOTAL", count("*").over(w3))
print("********************************************************************************\n")
#df1 = df1.withColumn("Frequency", col("CRASHESBYMAKE")/col("TOTAL"))
df1=df1.withColumn("FREQUENCY", col("CRASHESBYMAKE")/col("TOTAL"))
df1.show(n=30)
w4 = Window.orderBy("FREQUENCY")
df1=df1.withColumn("row_num", dense_rank().over(w4))
df1.show(n=30)

print("printed")

#df3 = df2.select('VEHICLE_MAKE', 'total_crashes', count('*').alias('dd'))
#df3.show()
#df3 = df2.select('VEHICLE_MAKE', 'total_crashes', sum(count('*')).over(window).alias('frequency'))
#df3.show()

# show the DataFrame

