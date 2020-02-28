package com.zhc.spark

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

/**
  * 自己管理kafka topic的offset
  */
object OffsetApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("OffsetApp")

    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val topics = Set[String]("kafka_streaming_topic")

    /**
      * 从指定位置获取到offset 放入下面的fromOffsets中
      */
    val fromOffsets = Map[TopicAndPartition, Long]()

    val messages = if (true) {
      //从头消费
      KafkaUtils
        .createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topics
      )
    } else {
      //从指定偏移量开始消费
      val messageHandler = (mm: MessageAndMetadata[String, String]) => (mm.key(), mm.message())

      KafkaUtils
        .createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        ssc, kafkaParams, fromOffsets, messageHandler
      )
    }

    /**
      * 在业务处理完，将偏移量存储到指定位置
      */
    messages.foreachRDD(rdd=>{

      //这是业务逻辑
      println(rdd.count())

      /**
        * 将offset 获取到，存入指定位置
        */

      val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      ranges.foreach(x=>{
        val topic: String = x.topic
        val partition: Int = x.partition
        val offset: Long = x.untilOffset

        //将 topic,partition,offset 存储进去
        (topic,partition,offset)
      })


    })

    ssc.start()
    ssc.awaitTermination()

  }

}
