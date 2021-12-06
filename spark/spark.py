import os
import time
from typing import Dict, List

from pyspark.sql import SparkSession, Window
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json, window, col, to_json, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


INGRESS_MSG_SCHEMA = StructType([
    StructField("httpVersion", StringType(), False),
    StructField("userAgent", StringType(), True),
    StructField("sourceIp", StringType(), False),
])

EGRESS_MSG_SCHEMA = StructType([
    StructField("userAgentCounts", ArrayType(StringType()), False)
])

KAFKA_BROKER_HOST = os.getenv("KAFKA_BROKER")
KAFKA_BROKER_PORT = os.getenv("KAFKA_BROKER_PORT")
KAFKA_INGRESS_TOPIC = os.getenv("KAFKA_INGRESS_TOPIC")
KAFKA_EGRESS_TOPIC = os.getenv("KAFKA_EGRESS_TOPIC")

def get_counts(msg: DataFrame, countable_cols: List[str], interval=None) -> List[DataFrame]:
    res = []
    for col_name in countable_cols:
        if interval:
            res.append((msg.withWatermark("timestamp", "5 second")
                           .groupBy(col_name, window("timestamp", interval, interval))
                           .count()))
        else:
            res.append(msg.groupBy(col_name).count())
    return res

def set_output_kafka(msgs: List[DataFrame], checkpoints: List[str], interval=None):
    for msg, cp in zip(msgs, checkpoints):
        out = (msg.writeStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", f"{KAFKA_BROKER_HOST}:{KAFKA_BROKER_PORT}")
                  .option("topic", KAFKA_EGRESS_TOPIC)
                  .option("checkpointLocation", f"/tmp/{cp}"))
        if interval:
            out = out.outputMode("update")
        else:
            out = out.outputMode("complete")
        out.start()

def format_output(msgs: List[DataFrame], data_types: List[str], interval=None):
    res = []
    for msg, data_type in zip(msgs, data_types):
        res.append((msg.withColumn("data_type", lit(data_type))
                       .select(to_json(struct("*")).alias("value"))))
    return res

def main():
    countable_cols = ["userAgent", "httpVersion", "sourceIp"]
    data_types_30m = [f"{col_}-count-window-30m" for col_ in countable_cols]
    data_types_8h = [f"{col_}-count-window-8h" for col_ in countable_cols]
    data_types_24h = [f"{col_}-count-window-24h" for col_ in countable_cols]
    data_types_running = [f"{col_}-count-window-running" for col_ in countable_cols]

    spark = SparkSession.builder.appName("nginx-aggregations").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    input = spark.readStream.format("kafka").option("kafka.bootstrap.servers", f"{KAFKA_BROKER_HOST}:{KAFKA_BROKER_PORT}").option("subscribe", KAFKA_INGRESS_TOPIC).load()

    msg_raw = input.selectExpr("CAST(value AS STRING)", "timestamp")
    msg = msg_raw.select(from_json("value", INGRESS_MSG_SCHEMA).alias("data"), "timestamp").select("data.*", "timestamp")

    cnts_running = get_counts(msg, countable_cols)
    cnts_30m = get_counts(msg, countable_cols, interval="30 minute")
    cnts_8h = get_counts(msg, countable_cols, interval="8 hour")
    cnts_24h = get_counts(msg, countable_cols, interval="24 hour")

    x = format_output(cnts_running, data_types_running)

    set_output_kafka(x, data_types_running)

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
