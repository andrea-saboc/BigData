#!/bin/bash
docker-compose -f Database/docker-compose.yml up
docker-compose -f HDFS/docker-compose.yml up
docker-compose -f Kafka/docker-compose.yml up
docker-compose -f Spark/docker-compose.yml up




docker exec -it spark-master ./spark/bin/spark-submit ../home/batch_primeri/app.py
docker exec -it spark-master ./spark/bin/spark-submit ../home/consumers/transformation_zone.py
docker exec -it spark-master ./spark/bin/spark-submit --jars ../home/consumers/postgresql-42.5.1.jar ../home/consumers/batch_processing.py
docker exec -it spark-master ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars ../home/consumers/postgresql-42.5.1.jar ../home/consumers/RealTime//01-real_time_processing.py

docker exec -it spark-master -c "pip3 install home/consumers/turfpy-0.0.7.tar.gz"
docker exec -it spark-master ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --py-files ../home/consumers/turfpy-0.0.7.tar.gz --jars ../home/consumers/postgresql-42.5.1.jar  ../home/consumers/RealTime//02-real_time_processing.py
docker exec -it spark-master ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --py-files ../home/consumers/turfpy-0.0.7.tar.gz --jars ../home/consumers/postgresql-42.5.1.jar  ../home/consumers/RealTime//03-real_time_processing.py


docker exec -it spark-master ./spark/bin/spark-submit --jars ../home/consumers/postgresql-42.5.1.jar ../home/consumers/try.py




