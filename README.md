# Spark at Scale
 
This demo simulates a stream of movie ratings.  Data flows from akka -> kafka -> spark streaming -> cassandra

## Kafka Setup 

[See the Kafka Setup Instructions in the KAFKA_SETUP.md file](KAFKA_SETUP.md)

## Download and load the movielens data

* Download the movielens 10 million ratings data set from http://grouplens.org/datasets/movielens/

* copy the movie data (movies.data and ratings.data) into the data directory

* [Follow the setup instructions in the LoadMovieData / MOVIE_DATA_README.md](LoadMovieData/MOVIE_DATA_README.md)

* Readme also has an option demo of making the movie lens data searchable with Solr

## Setup Akka Feeder

* build the feeder fat jar   
`sbt feeder/assembly`

* run the feeder

Copy the application.conf file to dev.conf and modify the zookeeper location and kafka topic accordingly.  Then override the configs by using -Dconfig.file=dev.conf to use the new config.

`java -Xmx5g -Dconfig.file=dev.conf -jar feeder/target/scala-2.10/feeder-assembly-1.0.jar 1 100 emailFeeder 2>&1 1>feeder-out.log &`



## Run Spark Streaming

* build the streaming jar
`sbt streaming/package`

* running locally for development
`spark-submit --packages org.apache.spark:spark-streaming-kafka-assembly_2.10:1.4.1 --class sparkAtScale.StreamingRatings streaming/target/scala-2.10/streaming_2.10-1.0.jar`
 
 * running on a server in foreground
 `dse spark-submit --packages org.apache.spark:spark-streaming-kafka-assembly_2.10:1.4.1 --class sparkAtScale.StreamingDirectEmails ./streaming/target/scala-2.10/streaming_2.10-0.1.jar 10.200.185.103:9092 true emails_checkpoint <maxRatePerPartition> <batchInterval> <kafkaOffsetResetType>`
 
* running on the server for production mode
`nohup spark-submit --packages org.apache.spark:spark-streaming-kafka-assembly_2.10:1.4.1 --class sparkAtScale.StreamingRatings streaming/target/scala-2.10/streaming_2.10-1.0.jar 2>&1 1>streaming-out.log &`
