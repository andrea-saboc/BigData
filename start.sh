#!/bin/bash
#docker container exec -it namenode bash -c "hdfs dfs -rm -r -f /raw"
#docker exec -it namenode bash -c "printf \nCOPY FILE TO HDFS\n"
#docker exec -it namenode bash -c "hdfs dfs -mkdir raw"
#docker exec -it namenode bash -c "hdfs dfs -put home/Batch/traffic_crashes.csv /user/root/raw/"
#docker exec -it namenode bash -c "hdfs dfs -put home/Batch/people.csv /user/root/raw/"


#docker exec -it spark-master ./spark/bin/spark-submit ../home/batch_primeri/app.py
#docker exec -it spark-master ./spark/bin/spark-submit ../home/consumers/transformation_zone.py
#docker exec -it spark-master ./spark/bin/spark-submit --jars ../home/consumers/postgresql-42.5.1.jar ../home/consumers/batch_processing.py
docker exec -it spark-master ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars ../home/consumers/postgresql-42.5.1.jar ../home/consumers/real_time_processing.py


#docker exec -it spark-master ./spark/bin/spark-submit --jars ../home/consumers/postgresql-42.5.1.jar ../home/consumers/try.py




