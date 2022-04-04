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

findspark.init('/home/martin96/Spark/spark-3.2.1-bin-hadoop3.2')
s3_client = boto3.client('s3')
sessions3 = boto3.Session()
s3 = sessions3.resource('s3')
my_bucket = s3.Bucket('pinterestkafkabucket')

def S3_to_Cassandra():
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

    os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.1.0 S3_Spark_Cassandra.py pyspark-shell'

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

    sp_table_string = sp_table_string[:(len(sp_table_string)-1)]
    #os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.1.0 spark_to_cassandra pyspark-shell'

    df = df.drop(df.index)
    # Add data to hbase table ## ALTER OutputMode to append!!!!
    df.write.format('org.apache.spark.sql.cassandra').mode('append') \
        .options(table="pinterest_data", keyspace="data") \
        .save()

    sessionsp.stop()

S3_to_Cassandra()





