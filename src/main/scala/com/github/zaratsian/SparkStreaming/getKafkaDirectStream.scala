
/***********************************************************************************************************************************
*
*   Spark Streaming - Test Stream
*
*   Usage:
*   getKafkaStream <zkQuorum> <group> <topics> <numThreads>
*       <zkQuorum> is a list of one or more zookeeper servers that make quorum
*       <group> is the name of kafka consumer group
*       <topics> is a list of one or more kafka topics to consume from
*       <numThreads> is the number of threads the kafka consumer should use    
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

import kafka.serializer.StringDecoder

object getKafkaDirectStream {
    def main(args: Array[String]) {

        val appname = props.getOrElse("appname", "getKafkaDirectStream") 
        val brokers = props.getOrElse("brokers", "sandbox.hortonworks.com:2181") 
        val topics  = props.getOrElse("topics",  "dztopic1")

        val sparkConf = new SparkConf().setAppName(appname)
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
