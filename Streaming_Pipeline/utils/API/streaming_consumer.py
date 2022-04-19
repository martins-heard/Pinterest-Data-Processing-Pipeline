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

class s_consumer:
    """
    This class will stream data generated from pinterest users (user emulator) to a cassandra  non-relational database.
    It will process and clean the data using spark and will use kafka to consume the data.

    Parameters
    ----------
    DATABASE_TYPE: str
        Type of database data is being sent to (postgresql)
    DBAPI: str
        Database API used to send data to database
    USER: str
        Username for postgres database
    PASSWORD: str
        Password for postgres database
    DATABASE: str
        Name of database
    PORT:
        Database port
    url: str
        url link to database

    """
    def __init__(self, USER: str = 'postgres', PASSWORD: str = 'Postgresql123!', 
            DATABASE: str = 'pinterest_project', PORT: int = 5432, url: str = 'jdbc:postgresql://localhost:5432/pinterest_project'):
        self.DATABASE_TYPE = 'postgresql'
        self.DBAPI = 'psycopg2'
        self.HOST = 'localhost'
        self.USER = USER
        self.PASSWORD = PASSWORD
        self.DATABASE = DATABASE
        self.PORT = PORT
        self.url = url

    def _process_data(self):
        """
        This is a private method which returns a Spark dataframe of processed and cleaned pinterest user data consumed using Kafka.
        
        Returns
        -------
            Spark Dataframe
                'category': str, 'unique_id': str, 'title': str, 'description': str, 'follower_count': int, 'tag_list': str,
                'is_image_or_video': str, 'image_src': str, 'downloaded': int, 'save_location': str
        """
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

        os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 utils/API/streaming_consumer.py pyspark-shell'
        kafka_topic = 'PinterestTopic'
        kafka_bootstrap_servers = 'localhost:9092'

        spark = SparkSession \
            .builder \
            .appName("Kafka") \
            .getOrCreate()

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
        # return df
        #df.select('tag_list').distinct().show()

        def _write_to_postgres(df, epoch_id):
            """
            This is a private method which writes the processed data to the postgres database.
            """
            mode="append"
            url = self.url
            properties = {"user": self.USER, "password": self.PASSWORD, "driver": "org.postgresql.Driver"}
            #df = self._process_data()
            df.write.jdbc(url=url, table="pinterest_data_2", mode=mode, properties=properties)
    
    #def main(self):
        """
        This is a method which initiates the write stream to send the processed data to the postgres database.
        """
        #df = self._process_data()
        df.writeStream \
            .format("jdbc") \
            .foreachBatch(_write_to_postgres) \
            .option("checkpointLocation", 'checkpoint_path/') \
            .outputMode('update') \
            .start().awaitTermination()
    
#if __name__ =="__main__":
consume = s_consumer()  
consume._process_data()
