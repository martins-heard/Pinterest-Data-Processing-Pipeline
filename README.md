# Pinterest-Data-Processing-Pipeline
## Description
This project processes user data from pinterest, cleans the data, and sends the data to a relational or non-relational database. This is done in two ways:

1. Batch processing - this process uses apache kafka to send the raw data from the pinterest user emulator to AWS S3 storage as a batch. From here the data is read and cleaned using Apache spark before being dumped into a cassandra or Hbase non-relational database. Apache Airflow manager has also been used to manage the workflow so data can be uploaded daily (assuming kafka is already set up and running). Apache Presto has been set up for adhoc querying.
### Batch Processing Order of Use
--- --project_pin_API.py--- > -----batch_consumer.py---- > -----------S3_Spark_Cassandra.py----------- > --batch_cassandra_workflow.py--> presto_pinterest.py
--- Listen to incoming data > Read incoming data into S3 > Read and clean data from S3 and write to db > Initiate workflow to run daily > query data in spark

2. Real-time processing - As opposed to the batch process, rather than storing the raw data in S3, the data is read and queried by spark as it is read by Kafka. Spark then cleans the data and writes it to a postgresql database.
### Real-Time Processing Order of Use    
--- --project_pin_API.py--- > --------------------------------------streaming_consumer.py -------------------------------------- 
--- listen to incoming data > Read the incoming data into a spark dataframe, clean the data and write into a postgresql database


## Milestones
This section describes the milestones for creatin the processes
### Milestone 1 Set up API and user emulator
The API for listening to events made by the user emulator was set up and tested.

### Milestone 2 Kafka Topic
The Kafka topic was created to consume incoming data and was tested to make sure the topic was recieving events from the API.

### Milestone 3 batch consumer
The batch consumer was created to send the data received from the kafka consumer to S3 using the boto3 package. Each event was saved as a seperate json file

### Milestone 4 Spark processing and cleaning data
The spark processor was created to read the data from S3 into a spark dataframe which then could be cleaned. At this stage the following columns were cleaned:
1. follower_count converted to integer
2. is_image_or_video converted to boolean
3. save_location trimmed to only contain file path
4. tag_list where there are no tags this has been changed to blank

### Milestone 5 Send data to Cassandra / Hbase
In the same script as milestone 4 the process writes the data into cassandra or hbase.

### Milestone 6 Presto
A script was created to allow for ad-hoc queries using presto.

### Milestone 7 Airflow
A workflow was created using airflow to allow the processing and writing to cassandra to occur daily.

### Milestone 8 Monitoring Cassandra
Cassandra was connected to prometheus for monitoring cassandra. This was visualised using grafana.

### Milestone 9 Spark-kafka integration
A script was created so events could be read directly from kafka into a spark dataframe.

### Milestone 10 real-time cleaning
The above script was updated to clean the date as per milestone 4.

### Milestone 11 Store in postgres
The above script was updated so after the data was cleaned it would be wtritten into a local postgres database.

### Milestone 12 Monitoring postgres 
Postgres was connected to prometheus to allow for monitoring. This was visualised in grafana.