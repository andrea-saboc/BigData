from json import dumps
from os import environ
import requests
import time
import json
import datetime
from datetime import datetime, timedelta
import pytz


from kafka import KafkaProducer

MAX_ENTRIES = 50
DATASET_API_LINK = environ.get("DATASET_API_LINK","https://data.cityofchicago.org/resource/n4j6-wkkf.json" )
KAFKA_TOPIC = environ.get("KAFKA_TOPIC", "chicago-traffic")
KAFKA_CONFIGURATION = {
    "bootstrap_servers": environ.get("KAFKA_BROKER", "kafka1:19092").split(","),
    "key_serializer": lambda x: str.encode("" if not x else x, encoding='utf-8'),
    "value_serializer": lambda x: dumps(dict() if not x else x).encode(encoding='utf-8'),
    "reconnect_backoff_ms": int(100)
}

def send_to_kafka(data):
    try:
        connecting = True
        entries = 0
        number_of_sent =0
        while connecting and entries<MAX_ENTRIES:
            try:
                print("Configuration")
                print(KAFKA_CONFIGURATION)
                producer = KafkaProducer(**KAFKA_CONFIGURATION)
                if producer.bootstrap_connected():
                    connecting = False
            except Exception as e:
                entries +=1
                print(f"Kafka-producer connection error: {e}")
            print(f"Connecting to Kafka ({entries})...")

        if entries>=MAX_ENTRIES:
            print("Cannot connect to Kafka.")
            return

        print(f"Kafka successfullly connected. Connected to bootsrap servers.")
        for segment in data:
            if "_last_updt" in segment:
                last_updated_time = datetime.strptime(segment['_last_updt'], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=pytz.timezone('America/Chicago'))
                time_difference = datetime.now(pytz.timezone('America/Chicago')) - last_updated_time
                print(time_difference)

                if time_difference > timedelta(minutes=30):
                    print("Skipping segment because it was not updated within the last 20 minutes.")
                    continue
            message= segment
            #message = json.dumps(segment)
            producer.send(topic=KAFKA_TOPIC, key=segment.get("_last_updt", None), value=message)
            print(f"Sent segment {message}.")
            number_of_sent +=1
        producer.flush() 
        print(f"Number of sent segments {number_of_sent}.")
        time.sleep(20)   
    except Exception as e:
        print(f"Error: {e}.")
        
def main():
    while True:
        try:
            DATASET_API_LINK = "https://data.cityofchicago.org/resource/n4j6-wkkf.json"

            print(DATASET_API_LINK)
            print("here")
            response = requests.get(url=DATASET_API_LINK)
            print(response.content[2])
            response = requests.get(url=DATASET_API_LINK)
            print("response is here")
            if response.ok:
                data = response.json()
                print("before sending")
                send_to_kafka(data)
            else:
                raise Exception((f"Response status code: {response.status_code}\nResponse text: {response.text}"))
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
        except Exception as e:
            print(f"OTHER ERROR:{e}")


if __name__ == "__main__":
    main()