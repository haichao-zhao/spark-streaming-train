package com.zhc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming整合Flume pull方式
  */
object FlumePullWordCount {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("FlumePullWordCount")

    val ssc = new StreamingContext(conf, Seconds(5))

    val flumeStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, "localhost", 41414)

    flumeStream
      .map(x => new String(x.event.getBody.array()).trim)
      .flatMap(_.split(" ")).map((_, 1))
      .reduceByKey(_ + _)
      .print()

    ssc.start()
    ssc.awaitTermination()

  }
}
