from cassandra.cluster import Cluster

new_cluster = Cluster()
session = new_cluster.connect('data')

# session.execute(''' DROP TABLE data.pinterest_data;''')

session.execute(''' CREATE TABLE pinterest_data (
    category text,
    unique_id text,
    title text,
    description text,
    follower_count text,
    tag_list text,
    is_image_or_video text,
    image_src text,
    downloaded int,
    save_location text,
    PRIMARY KEY (unique_id)
    );''')


# data_spark_schema = ArrayType(StructType([
#         StructField("category", StringType(), True), \
#         StructField("unique_id", StringType(), True), \
#         StructField("title", StringType(), True), \
#         StructField("description", StringType(), True), \
#         StructField("follower_count", StringType(), True), \
#         StructField("tag_list", StringType(), True), \
#         StructField("is_image_or_video", StringType(), True), \
#         StructField("image_src", StringType(), True), \
#         StructField("downloaded", IntegerType(), True), \
#         StructField("save_location",StringType(), True)]))