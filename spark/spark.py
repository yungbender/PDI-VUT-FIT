import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType


INGRESS_MSG_SCHEMA = StructType([
    StructField("httpVersion", StringType(), False),
    StructField("userAgent", StringType(), True),
    StructField("sourceIp", StringType(), False),
])

KAFKA_BROKER_HOST = os.getenv("KAFKA_BROKER")
KAFKA_BROKER_PORT = os.getenv("KAFKA_BROKER_PORT")
KAFKA_INGRESS_TOPIC = os.getenv("KAFKA_INGRESS_TOPIC")
KAFKA_EGRESS_TOPIC = os.getenv("KAFKA_EGRESS_TOPIC")

def main():
    spark = SparkSession.builder.appName("spark-test").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    input = spark.readStream.format("kafka").option("kafka.bootstrap.servers", f"{KAFKA_BROKER_HOST}:{KAFKA_BROKER_PORT}").option("subscribe", KAFKA_INGRESS_TOPIC).load()

    msg_raw = input.selectExpr("CAST(value AS STRING)")
    msg = msg_raw.select(from_json("value", INGRESS_MSG_SCHEMA).alias("data")).select("data.*")

    msg = msg.groupBy("userAgent").count()

    msg.writeStream.format("console").outputMode("complete").start().awaitTermination()


if __name__ == "__main__":
    main()
