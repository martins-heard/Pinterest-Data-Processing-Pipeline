import os
import json
import boto3
import pyspark
import findspark
import pandas as pd
import multiprocessing
from functools import reduce
from pyspark.sql import SparkSession
from pyspark import SQLContext, SparkContext, SparkConf
from cassandra.cluster import Cluster
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, BooleanType

def S3_to_Cassandra():
    findspark.init('/home/martin96/Spark/spark-3.2.1-bin-hadoop3.2')
    s3_client = boto3.client('s3')
    sessions3 = boto3.Session()
    s3 = sessions3.resource('s3')
    my_bucket = s3.Bucket('pinterestkafkabucket')

    conf = (
    pyspark.SparkConf()
    # Setting where master node is located [cores for multiprocessing]
    .setMaster(f"local[{multiprocessing.cpu_count()}]")
    # Setting application name
    .setAppName("PinterestApp")
    # Setting config value via string
    .set("spark.eventLog.enabled", False)
    # Setting environment variables for executors to use
    .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
    # Setting memory if this setting was not set previously
    .setIfMissing("spark.executor.memory", "1g")
    )

    os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.1.0 Pinterest_App/S3_Spark_Cassandra.py pyspark-shell'

    sessionsp = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
    sc = sessionsp.sparkContext

    # Read Data from S3 bucket to pyspark dataframe
    df_list = []
    for obj in my_bucket.objects.all():
        obj = s3_client.get_object(Bucket='pinterestkafkabucket', Key=obj.key)
        content = obj['Body'].read()
        data = json.loads(content)
        pkeys = sc.parallelize([data])
        norm = pd.json_normalize(pkeys.collect())
        norm_list = norm.iloc[0].tolist()
        df = sessionsp.createDataFrame(pd.DataFrame(norm, columns=norm.keys()))
        df_list.append(df)
    df = reduce(pyspark.sql.DataFrame.union, df_list)
    df.show()

    # Set up column mapping for hbase
    sp_table_string = "index int :key,"
    for col in df.columns:
        if col != "index":
            if col == "downloaded":
                sp_table_string += f"{col} int Incoming_Data:{col},"
            else:
                sp_table_string += f"{col} STRING Incoming_Data:{col},"

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

    df = df.drop(df.index)
    df.show()

    # Add dataframe to cassandra
    df.write.format('org.apache.spark.sql.cassandra').mode('append') \
        .options(table="pinterest_data", keyspace="data") \
        .save()

    sessionsp.stop()

S3_to_Cassandra()





