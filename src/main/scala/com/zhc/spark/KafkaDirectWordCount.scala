package com.zhc.spark

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming对接Kafka的方式二
  */
object KafkaDirectWordCount {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaReceiverWordCount")

    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaParams = Map[String,String]("metadata.broker.list" -> "localhost:9092")
    val topics = Set[String]("kafka_streaming_topic")


    val messages: InputDStream[(String, String)] = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics
    )

    messages.map(_._2).flatMap(_.split(" "))
      .map((_, 1)).reduceByKey(_ + _)
      .print()

    ssc.start()
    ssc.awaitTermination()

  }

}
