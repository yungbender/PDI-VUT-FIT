"""
Spark job for aggregating nginx stats.

Authors: Tomas Sasak (xsasak01), Jakub Frejlach (xfrejl00)
"""
import os
from typing import List

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json, window, to_json, struct, lit, avg, udf
from pyspark.sql.types import IntegerType, StructType, StructField, StringType 
from user_agents import parse

# Schema of kafka json input message (spark-ingress)
INGRESS_MSG_SCHEMA = StructType([
    StructField("httpVersion", StringType(), False),
    StructField("userAgent", StringType(), True),
    StructField("sourceIp", StringType(), False),
    StructField("contentLength", IntegerType(), False),
    StructField("contentType", StringType(), False),
])

# Get kafka and spark attributes
KAFKA_BROKER_HOST = os.getenv("KAFKA_BROKER")
KAFKA_BROKER_PORT = os.getenv("KAFKA_BROKER_PORT")
KAFKA_INGRESS_TOPIC = os.getenv("KAFKA_INGRESS_TOPIC")
KAFKA_EGRESS_TOPIC = os.getenv("KAFKA_EGRESS_TOPIC")
SPARK_MASTER_HOST = os.getenv("SPARK_MASTER_HOST")
SPARK_MASTER_PORT = os.getenv("SPARK_MASTER_PORT")


def get_counts(msg: DataFrame, countable_cols: List[str], interval=None) -> List[DataFrame]:
    """
    Calculates counts from given columns in optional window interval.
    """
    res = []
    for col_name in countable_cols:
        if interval:
            res.append((msg.withWatermark("timestamp", "5 second")
                           .groupBy(col_name, window("timestamp", interval, interval))
                           .count()))
        else:
            res.append(msg.groupBy(col_name).count())
    return res


def get_averages(msg: DataFrame, averagable_cols: List[str], interval=None) -> List[DataFrame]:
    """
    Calculates average from given columns in optional window interval.
    """
    res = []
    for col_name in averagable_cols:
        if interval:
            msg = (msg.withWatermark("timestamp", "5 second")
                           .groupBy(window("timestamp", interval, interval))
                           .agg(avg(col_name).alias("average")))
        else:
            msg = msg.groupBy().agg(avg(col_name).alias("average"))
        # replace null value if there is no record in stream
        res.append(msg.fillna({"average": 0}))
    return res


def set_output_kafka(msgs: List[DataFrame], checkpoints: List[str], interval=None):
    """
    Sets kafka output sink with topics.
    """
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
    """
    Formats output for kafka with given data types for UI.
    """
    res = []
    for msg, data_type in zip(msgs, data_types):
        res.append((msg.withColumn("data_type", lit(data_type))
                       .select(to_json(struct("*")).alias("value"))))
    return res


def build_job():
    """
    Builds job with connection to the master spark node.
    """
    config = SparkConf()
    config.setAll([
        ("spark.master", f"spark://{SPARK_MASTER_HOST}:{SPARK_MASTER_PORT}"),
        ("spark.driver.host", "pdi-bootstrapper"),
        ("spark.submit.deployMode", "cluster"),
        ("spark.driver.bindAddress", "0.0.0.0"),
        ("spark.app.name", "nginx-aggregations"),
        ("spark.yarn.submit.waitAppCompletion", "false"),
        ("spark.executor.memory", "2GB"),
    ])

    return SparkSession.builder.config(conf=config).getOrCreate()


def parse_browser(ua_str: str):
    """
    Parses browser type from user-agent string.
    """
    if ua_str is None or ua_str == "Not Defined":
        return ua_str

    try:
        ua = parse(ua_str)
    except Exception as exc:
        return "Not Defined"
    return ua.browser.family


def main():
    # this columns are counter from input kafka msg
    countable_cols = ["userAgent", "httpVersion", "sourceIp", "contentType"]
    averageable_cols = ["contentLength"]

    # this are output data types, this will ui get
    data_types_30m = [f"{col_}-count-window-30m" for col_ in countable_cols] + [f"{col_}-average-window-30m" for col_ in averageable_cols]
    data_types_8h = [f"{col_}-count-window-8h" for col_ in countable_cols] + [f"{col_}-average-window-8h" for col_ in averageable_cols]
    data_types_24h = [f"{col_}-count-window-24h" for col_ in countable_cols] + [f"{col_}-average-window-24h" for col_ in averageable_cols]
    data_types_running = [f"{col_}-count-window-running" for col_ in countable_cols] + [f"{col_}-average-window-running" for col_ in averageable_cols]

    # create spark job
    spark = build_job()
    spark = SparkSession.builder.appName("nginx-aggregations").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # create custom SQL function to parse browser from user-agent string
    browser = udf(lambda x: parse_browser(x), StringType())

    # set kafka input
    input = spark.readStream.format("kafka").option("kafka.bootstrap.servers", f"{KAFKA_BROKER_HOST}:{KAFKA_BROKER_PORT}").option("subscribe", KAFKA_INGRESS_TOPIC).load()

    # parse msg from kafka to json
    msg_raw = input.selectExpr("CAST(value AS STRING)", "timestamp")
    msg = msg_raw.select(from_json("value", INGRESS_MSG_SCHEMA).alias("data"), "timestamp").select("data.*", "timestamp")


    # fillout null values if nginx cannot specify them
    msg = msg.fillna({"httpVersion": "Not Defined",
                      "userAgent": "Not Defined",
                      "sourceIp": "Not Defined",
                      "contentLength": 0,
                      "contentType": "Not Defined"})

    # parse user-agent
    msg = msg.withColumn("userAgent", browser("userAgent"))

    # create count queries, for countable columns
    cnts_running = get_counts(msg, countable_cols)
    cnts_30m = get_counts(msg, countable_cols, interval="30 minute")
    cnts_8h = get_counts(msg, countable_cols, interval="8 hour")
    cnts_24h = get_counts(msg, countable_cols, interval="24 hour")

    # create avg queries, for averagable columns
    avgs_running = get_averages(msg, averageable_cols)
    avgs_30m = get_averages(msg, averageable_cols, interval="30 minute")
    avgs_8h = get_averages(msg, averageable_cols, interval="8 hour")
    avgs_24h = get_averages(msg, averageable_cols, interval="24 hour")

    # format the output dataframes for kafka
    out_running = format_output(cnts_running + avgs_running, data_types_running)
    out_30m = format_output(cnts_30m + avgs_30m, data_types_30m, interval="30 minute")
    out_8h = format_output(cnts_8h + avgs_8h, data_types_8h, interval="8 hour")
    out_24h = format_output(cnts_24h + avgs_24h, data_types_24h, interval="24 hour")

    # set output sink to kafka with topic
    set_output_kafka(out_running, data_types_running)
    set_output_kafka(out_30m, data_types_30m, interval="30 minute")
    set_output_kafka(out_8h, data_types_8h, interval="8 hour")
    set_output_kafka(out_24h, data_types_24h, interval="24 hour")

    # start job
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
