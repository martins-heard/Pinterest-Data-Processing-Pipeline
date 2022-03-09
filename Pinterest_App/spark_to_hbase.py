import findspark
import multiprocessing
from pyspark.sql import SparkSession
from pyspark import SQLContext, SparkContext, SparkConf

findspark.init('/home/martin96/Spark/spark-3.2.1-bin-hadoop3.2')
findspark.find()

def write_to_hbase():
    conf = (SparkConf().setAppName("RW_from_HBase"))

    spark = SparkSession.builder.appName(" ").config(conf=conf).getOrCreate()

    #df = spark.read.options(HBaseTableCatalog.tableCatalog -> catalog)
    
    df = spark.read.format("org.apache.hadoop.hbase.spark").option("hbase.columns.mapping", #.write to write to hbase
            "rowKey STRING :key," +
            "firstName STRING Name:First, LastName STRING Name:Last," +
            "country STRING Address:Country, state STRING Address:State"
            ).option("hbase.spark.pushdown.columnfilter", True).option("hbase.table", "retail_table").load()

    df.show()

write_to_hbase()