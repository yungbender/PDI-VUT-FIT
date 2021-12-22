import os
import subprocess
from time import sleep

from kafka import KafkaConsumer

KAFKA_BROKER_HOST = os.getenv("KAFKA_BROKER")
KAFKA_BROKER_PORT = os.getenv("KAFKA_BROKER_PORT")
SPARK_MASTER_HOST = os.getenv("SPARK_MASTER_HOST")
SPARK_MASTER_PORT = os.getenv("SPARK_MASTER_PORT")


def main():
    # check if kafka is running
    print("Checking if kafka is running...")
    while True:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=f"{KAFKA_BROKER_HOST}:{KAFKA_BROKER_PORT}",
                group_id=None,
            )
            consumer.close()
            break
        except Exception as exc:
            print(exc)
            print("Cannot connect to kafka, retrying")
            sleep(2)
    print("Kafka running, starting spark job...")
    # if kafka is running start spark job
    while True:
        try:
            process = subprocess.run(
                ["spark-submit", "./spark.py",
                 "--master", f"{SPARK_MASTER_HOST}:{SPARK_MASTER_PORT}",
                 ],
                 capture_output=True,
                 text=True,
                 check=True
            )
            break
        except subprocess.CalledProcessError as exc:
            print(exc)
            print("Cannot submit spark, retrying")
            sleep(2)
    print("Job done, exiting")


if __name__ == "__main__":
    main()
