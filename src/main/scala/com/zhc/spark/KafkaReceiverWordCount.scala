package com.zhc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming对接Kafka的方式一
  */
object KafkaReceiverWordCount {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaReceiverWordCount")

    val ssc = new StreamingContext(conf, Seconds(5))

    val topics = Map("kafka_streaming_topic" -> 1)
    println(topics)

    val messages: ReceiverInputDStream[(String, String)] = KafkaUtils
      .createStream(ssc, "localhost:2181", "test", topics)

    messages.map(_._2).flatMap(_.split(" "))
      .map((_, 1)).reduceByKey(_ + _)
      .print()

    ssc.start()
    ssc.awaitTermination()

  }

}
