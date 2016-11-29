

/**********************************************************************************************************************


Usage (Docker):
/apache-maven-3.3.9/bin/mvn clean package
/spark/bin/spark-submit --master local[*] --class "sparkMapWithState" --jars /phoenix-spark-4.8.1-HBase-1.1.jar target/SparkStreaming-0.0.1.jar phoenix.dev:2181 mytestgroup dztopic1 1 kafka.dev:9092


This code assumes each Kafka event is pipe-delimited, such as the following:
DEVICE_ID|HEALTH_STATUS|METRIC1|METRIC2|METRIC3
101|0|10|20|30.1
102|0|40|50|60.2
103|0|70|80|90.3
101|1|10|20|30.1


Phoenix Jar found here:
wget http://central.maven.org/maven2/org/apache/phoenix/phoenix-spark/4.8.1-HBase-1.1/phoenix-spark-4.8.1-HBase-1.1.jar

**********************************************************************************************************************/


import java.util.HashMap
import java.util.Arrays
import java.sql.DriverManager

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import _root_.kafka.serializer.StringDecoder

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType,FloatType}

import org.apache.phoenix.spark._

import org.apache.spark.HashPartitioner


object sparkMapWithState {

   case class stateCase(sum: Double, red: Double, seen_keys: List[String])

   def main(args: Array[String]) {
      if (args.length < 5) {
         System.err.println("Usage: /spark/bin/spark-submit --master local[*] --class sparkMapWithState --jars /phoenix-spark-4.8.1-HBase-1.1.jar target/SparkStreaming-0.0.1.jar <zkQuorum> <group> <topics> <numThreads> <kafkabroker>")
         System.exit(1)
      }

      val batchIntervalSeconds = 10               // 10  seconds
      val slidingInterval = Duration(2000L)       // 2   seconds
      val windowSize = Duration(10000L)           // 10  seconds
      val checkpointInterval = Duration(120000L)  // 120 seconds

      val Array(zkQuorum, group, topics, numThreads, kafkabroker) = args
      val sparkConf = new SparkConf().setAppName("sparkMapWithState")
      val sc  = new SparkContext(sparkConf)
      val ssc = new StreamingContext(sc, Seconds(batchIntervalSeconds))
      ssc.checkpoint(".")

      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      // Approach 1: Kafka Receiver-based Approach (http://spark.apache.org/docs/1.6.0/streaming-kafka-integration.html)
      //val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
      //val events = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

      // Approach 2: Kafka Direct Approach
      val topicsSet = topics.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkabroker)
      val events = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet).map(_._2)

      /********************************************************************************************
      *
      *  Parse each Kafka event - Creates DStream
      *
      *********************************************************************************************/

      val event = events.map(_.split("\\|")).map(p =>   
          (p(0), (p(1).toInt,p(2).toInt,p(3).toInt,p(4).toFloat,

          // Create new field based on rule:
          if ( (p(1).toInt == 1) && (p(4).toFloat <= 50) ) "CAUTION" else "OK"

          ))
      )

      event.map(x => x._2 ).print()


      ssc.start()
      ssc.awaitTermination()
   
   }

}


//ZEND
