package sparkAtScale

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext, Time}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

case class Rating(user_id: Int, movie_id: Int, rating: Float, batchtime:Long)

object StreamingRatings {
  def main(args: Array[String]) {

    val conf = new SparkConf()

    val sc = SparkContext.getOrCreate(conf)

    def createStreamingContext(): StreamingContext = {
      @transient val newSsc = new StreamingContext(sc, Milliseconds(500))
      println(s"Creating new StreamingContext $newSsc")

      newSsc
    }
    val ssc = StreamingContext.getActiveOrCreate(createStreamingContext)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    if (args.length < 2) {
      print("Supply the kafka broker as the first parameter and whether to display debug output as the second parameter (true|false) ")
    }

    val brokers = args(0)
    val topics = Set("ratings")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    println(s"connecting to brokers: $brokers")
    val debugOutput = args(1).toBoolean

    val ratingsStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    ratingsStream.foreachRDD {
      (message: RDD[(String, String)], batchTime: Time) => {
        // convert each RDD from the batch into a Ratings DataFrame
        //rating data has the format user_id:movie_id:rating:timestamp
        val df = message.map {
          case (key, nxtRating) => nxtRating.split("::")
        }.map(rating => {
          val timestamp: Long = new DateTime(rating(3).trim.toLong).getMillis
          Rating(rating(0).trim.toInt, rating(1).trim.toInt, rating(2).trim.toFloat, timestamp)
        } ).toDF("user_id", "movie_id", "rating", "timestamp")

        // this can be used to debug dataframes
        if (debugOutput)
          df.show()

        // save the DataFrame to Cassandra
        // Note:  Cassandra has been initialized through spark-env.sh
        //        Specifically, export SPARK_JAVA_OPTS=-Dspark.cassandra.connection.host=127.0.0.1
        df.write.format("org.apache.spark.sql.cassandra")
          .mode(SaveMode.Append)
          .options(Map("keyspace" -> "movie_db", "table" -> "rating_by_movie"))
          .save()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
