import utils.API.batch_consumer as batch
import utils.S3_Spark_Cassandra as S3_Spark_Cassandra

b_consume = batch.b_consumer()
print('uploading batch to S3')
b_consume.main()# Consume data from user emulator and send to S3 Storage
print('upload to Cassandra')
s3_cql = S3_Spark_Cassandra.s3_to_cql() # from S3 storage clean and upload the data to cassandra
s3_cql.S3_to_Cassandra()
print('delete contents of s3 bucket')
s3_cql.delete_S3_contents() # delete contents from S3 bucket now it was been uploaded to cassandra



