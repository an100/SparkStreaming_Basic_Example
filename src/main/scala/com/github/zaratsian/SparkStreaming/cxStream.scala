
/**********************************************************************************************************************
*
*  Usage:
*  spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0 --class "cxStream" --master yarn-client ./target/SparkStreaming-0.0.1.jar seregion01.cloud.hortonworks.com:2181 mytestgroup topicdz1 1
*
**********************************************************************************************************************/

import java.util.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType,FloatType}

import org.apache.phoenix.spark._


object cxStream {
   def main(args: Array[String]) {
      if (args.length < 4) {
         System.err.println("Usage: coxStream <zkQuorum><group> <topics> <numThreads>")
         System.exit(1)
      }

      val Array(zkQuorum, group, topics, numThreads) = args
      val sparkConf = new SparkConf().setAppName("KafkaWordCount")
      val ssc = new StreamingContext(sparkConf, Seconds(15))
      ssc.checkpoint("checkpoint")


      val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
      val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
      //val record = lines.map(x => x.split(","))
      

      val record = lines.map(_.split(",")).map(p => Row(
        p(0),          p(1),          p(2),          p(3),          p(4).toInt, p(5),          p(6),  p(7),  p(8),          p(9).toFloat, 
        p(10).toFloat, p(11).toFloat, p(12),         p(13),         p(14),      p(15).toFloat, p(16), p(17), p(18).toFloat, p(19).toFloat,
        p(20).toInt,   p(21).toFloat, p(22).toFloat, p(23).toFloat, p(24).toInt))


      val schema = (new StructType).add("EQUIP_ADDR",StringType).add("SITE_ID",StringType).add("ACCT_NBR",StringType).add("SVC_CATG_CODE",StringType).add("SVC_OCURNC",IntegerType).add("ACTV_FLAG",StringType).add("HSE_ID",StringType).add("HSE_NBR",StringType).add("NODE",StringType).add("EQUIP_STATS_SNR_VAL",FloatType).add("EQUIP_STATS_TX_VAL",FloatType).add("EQUIP_STATS_RX_VAL",FloatType).add("EQUIP_STATS_DTM_GMT",StringType).add("SERL_NBR",StringType).add("CMTS_NAME",StringType).add("EQUIP_STATS_T3_VAL",FloatType).add("PORT_TYPE",StringType).add("ITEM_NBR",StringType).add("EQUIP_STATS_FEC_VAL",FloatType).add("CM_ICFR",FloatType).add("RIPPLES",IntegerType).add("UPSTRM_CHNL_BANDWIDTH",FloatType).add("PRE_EQ_TAP_9",FloatType).add("PRE_EQ_TAP_10",FloatType).add("IF_INDEX",IntegerType)


      val df = sqlContext.createDataFrame(record, schema)

      val df1 = df.withColumn("mer_device_flag", when( ($"EQUIP_STATS_SNR_VAL" < 32), 1).otherwise(0) )
      df1.select("EQUIP_STATS_SNR_VAL","mer_device_flag").show()

      val df2 = df1.withColumn("inhome_wire_issue_flag", when( ($"CM_ICFR" >= 4) and ($"RIPPLES" <= 2) and ($"PRE_EQ_TAP_9" >= -10) and ($"PRE_EQ_TAP_10" >= -15), 1).otherwise(0) )
      df2.select("EQUIP_STATS_SNR_VAL","CM_ICFR","RIPPLES","PRE_EQ_TAP_9","PRE_EQ_TAP_10","inhome_wire_issue_flag","mer_device_flag").show()


      // Save to HBase via Phoenix
      df2.save("org.apache.phoenix.spark", SaveMode.Overwrite, Map("table" -> "OUTPUT_TABLE", "zkUrl" -> "seregion01.cloud.hortonworks.com:2181/hbase-unsecure" ))     


      ssc.start()
      ssc.awaitTermination()
   }
}
