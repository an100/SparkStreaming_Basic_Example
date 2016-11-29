
/***********************************************************************************************************************************
*
*   Spark Streaming - Test Stream
*
*   Usage:
*   getKafkaDirectStream <brokers> <topics>
*       <brokers> is a list of one or more Kafka brokers                (ie. localhost:9092)
*       <topics> is a list of one or more kafka topics to consume from  (i.e. dztopic1)
*
* https://github.com/apache/spark/tree/master/examples/src/main/scala/org/apache/spark/examples/streaming
*
*
* /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --zookeeper sandbox.hortonworks.com:2181 --create --topic dztopic1 --partitions 2 --replication-factor 2
***********************************************************************************************************************************/

package com.github.zaratsian.SparkStreaming;

import org.apache.spark.streaming._
import org.apache.spark.SparkConf

import org.apache.spark.streaming.kafka._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import _root_.kafka.serializer.StringDecoder

object getKafkaDirectStream {
    def main(args: Array[String]) {

        if (args.length < 2) {
           System.err.println("Usage: getKafkaDirectStream <brokers> <topics>")
           System.exit(1)
        }

        val Array(brokers, topics) = args

        val sparkConf = new SparkConf().setAppName("getKafkaDirectStream")
        val ssc = new StreamingContext(sparkConf, Seconds(2))
        //ssc.checkpoint("checkpoint")

        val topicsSet   = topics.split(",").toSet
        val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

        val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

        val lines = messages.map(_._2)
        val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
        wordCounts.print()

        ssc.start()
        ssc.awaitTermination()

   }

}

//ZEND
