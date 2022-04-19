import time
#import Pinterest_App.API.batch_consumer as batch
#import Pinterest_App.S3_Spark_Cassandra as S3_Spark_Cassandra
import Pinterest_App.API.streaming_consumer as stream

# b_consume = batch.b_consumer()
# # Consume data from user emulator and send to S3 Storage
# print('uploading to S3')
# b_consume._main()
# print('finished uploading...')
# print('Now writing to cassandra...')
# write_to_Cassandra = S3_Spark_Cassandra.S3_to_Cassandra()
# # Write the data in S3 to cassandra and process the data
# write_to_Cassandra()
# print('Finished writing to cassandra')


#print("now streaming")
s_stream = stream.s_consumer()
s_stream._process_data()



