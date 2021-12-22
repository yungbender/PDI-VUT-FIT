"""
Bootstrap script for starting whole app.

Authors: Tomas Sasak (xsasak01), Jakub Frejlach (xfrejl00)
"""
import os
import subprocess
import requests
from time import sleep

from kafka import KafkaConsumer

# Kafka and spark attributes for connection
KAFKA_BROKER_HOST = os.getenv("KAFKA_BROKER")
KAFKA_BROKER_PORT = os.getenv("KAFKA_BROKER_PORT")
SPARK_MASTER_HOST = os.getenv("SPARK_MASTER_HOST")
SPARK_MASTER_PORT = os.getenv("SPARK_MASTER_PORT")
SPARK_MASTER_STATUS_PORT = os.getenv("SPARK_MASTER_STATUS_PORT")


def main():
    """
    Script waits until kafka is running.
    If it is running, checks if spark master and worker nodes are running.
    If everything is ready, submits the job.
    If job crashes it is always restarted.
    """
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
            print("Cannot connect to kafka, retrying...")
            sleep(2)
    # check if spark is running
    print("Kafka running, checking if spark master is running...")
    while True:
        try:
            status_page = str(requests.get(f"http://{SPARK_MASTER_HOST}:{SPARK_MASTER_STATUS_PORT}/").content)
            if not "Workers" in status_page or "Workers (0)" in status_page:
                raise Exception("Spark does not have any workers connected")
            break
        except Exception as exc:
            print(exc)
            print("Cannot connect to spark, retrying...")
            sleep(2)

    print("Spark running, starting spark job...")
    # if kafka is running start spark job
    while True:
        try:
            process = subprocess.run(
                ["spark-submit",
                 "--py-files", "./job-requirements.zip",
                 "./spark.py",
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
