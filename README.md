# Spark at Scale
 
This demo simulates a stream of email metadata.  Data flows from akka -> kafka -> spark streaming -> cassandra

### Kafka Setup 

[See the Kafka Setup Instructions in the KAFKA_SETUP.md file](KAFKA_SETUP.md)

### Setup the KS/table

**Note: You can change RF and compaction settings in this cql script if needed.**

`cqlsh -f /path_to_SparkAtScale/LoadMovieData/conf/email_db.cql` 


### Setup Akka Feeder

###### Build the feeder fat jar   
`sbt feeder/assembly`

Edit kafkaHost and kafkaTopic if needed. kafaHost should match the setting in kafka/conf/server.properties and kafkaTopic should match that used when creating the topic.

###### Run the feeder

Parameters:

1. Number of feeders to start 

2. Time interval (ms) between sent requests by each feeder (1 feeder sending a message every 100 ms will equate to 10 messages/sec)

3. Feeder name

**Note: You will want to update the KafkaHost param in dev.conf to match settings in kafka/conf/server.properties**
`java -Xmx5g -Dconfig.file=dev.conf -jar feeder/target/scala-2.10/feeder-assembly-0.1.jar 1 100 emailFeeder 2>&1 1>feeder-out.log &`


### Run Spark Streaming

###### Build the streaming jar
`sbt streaming/assembly`

**Note: You will want to reference the correct Spark version, for example running against Spark 1.4 use 1.4.1 instead of 1.5.0**

Parameters:

1. kafka broker: Ex. 10.200.185.103:9092 

2. debug flag (limited use): Ex. true or false 

3. checkpoint directory name: Ex. cfs://[optional-ip-address]/emails_checkpoint, dsefs://[optional-ip-address]/emails_checkpoint

4. [spark.streaming.kafka.maxRatePerPartition](http://spark.apache.org/docs/latest/configuration.html#spark-streaming): Maximum rate (number of records per second) 

5. batch interval (ms) 

6. [auto.offset.reset](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.kafka.KafkaUtils$): Ex. smallest or largest

7. topic name 

8. kafka stream type: ex. direct or receiver

9. number of receivers to create (controls read parallelism) (receiver approach: typically this should be the number of nodes in the cluster) 

10. processesing parallelism (controls write parallelism) (receiver approach: you'll want to match whatever used when creating the topic) 

11. group.id that id's the consumer processes (receiver approach: you'll want to match whatever used when creating the topic) 

12. zookeeper connect string (e.g localhost:2181) (receiver approach: you'll want to match whatever used when creating the topic) 

###### Running on a server in foreground
`dse spark-submit --driver-memory 2G --class sparkAtScale.StreamingDirectEmails streaming/target/scala-2.10/streaming-assembly-0.1.jar <kafka-broker-ip>:9092 true dsefs://[optional-ip-address]/emails_checkpoint 50000 5000 smallest emails direct 1 100 test-consumer-group localhost:2181`
