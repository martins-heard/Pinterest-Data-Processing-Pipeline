# from matplotlib.font_manager import json_load
from pyspark.sql import SparkSession
import pandas as pd
import os
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import json
from kafka import KafkaConsumer
import time
import re
import json
from pyspark.sql import Row
import psycopg2
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, IntegerType, MapType
from pyspark.sql import functions as F
from sqlalchemy import create_engine
import pandas as pd

DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'
HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'Postgresql123!'
DATABASE = 'pinterest_project'
PORT = 5432

data_spark_schema = ArrayType(StructType([
        StructField("category", StringType(), True), \
        StructField("unique_id", StringType(), True), \
        StructField("title", StringType(), True), \
        StructField("description", StringType(), True), \
        StructField("follower_count", IntegerType(), True), \
        StructField("tag_list", StringType(), True), \
        StructField("is_image_or_video", StringType(), True), \
        StructField("image_src", StringType(), True), \
        StructField("downloaded", IntegerType(), True), \
        StructField("save_location",StringType(), True)]))

os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 streaming_consumer.py pyspark-shell'
kafka_topic = 'PinterestTopic'
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession \
    .builder \
    .appName("Kafka") \
    .getOrCreate()

mapCol = MapType(StringType(), StringType(), False)

data_df = spark \
    .readStream \
    .format("Kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()


df = data_df.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)").withColumn("value",F.explode(F.from_json("value", data_spark_schema))).select("value.*")

# Clean Follower Count - convert to integer
df = df.withColumn('follower_count', F.regexp_replace(df.follower_count, 'k', '000'))
df = df.withColumn('follower_count', F.regexp_replace(df.follower_count, 'M', '000000'))
df = df.withColumn('follower_count', F.regexp_replace(df.follower_count, 'User Info Error', '0').cast(IntegerType()))
#df.select('follower_count').distinct().show()

# Clean is image or video - convert to boolean
df = df.withColumn('is_image_or_video', F.regexp_replace(df.is_image_or_video, 'multi-video(story page format)', 'video'))
#df.select('is_image_or_video').distinct().show()

# Clean save location - remove 'local save in'
df = df.withColumn('save_location', F.regexp_replace(df.save_location, 'Local save in ', ''))

# Clean tag list - remove N,o, ,T,a,g,s
df = df.withColumn('tag_list', F.regexp_replace(df.tag_list, 'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e', ''))
#df.select('tag_list').distinct().show()

def write_to_postgres(df, epoch_id):
    mode="append"
    url = "jdbc:postgresql://localhost:5432/pinterest_project"
    properties = {"user": USER, "password": PASSWORD, "driver": "org.postgresql.Driver"}
    df.write.jdbc(url=url, table="pinterest_data_2", mode=mode, properties=properties)

df.writeStream \
    .format("jdbc") \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", 'checkpoint_path/') \
    .outputMode('update') \
    .start().awaitTermination()

#my_vals.writeStream.outputMode("append").format("console").start().awaitTermination()
#my_df.writeStream.option("checkpointLocation", "checkpoint/").option("path", "output_path/").outputMode("append").format("csv").trigger(processingTime="10 seconds").start().awaitTermination()
