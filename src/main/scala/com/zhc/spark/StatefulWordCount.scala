package com.zhc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming 完成带状态统计
  */
object StatefulWordCount {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("StatefulWordCount")

    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("file:///Users/zhaohaichao/data/tmp/")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 6789)

    val res: DStream[(String, Int)] = lines.flatMap(_.split(",")).map((_, 1))
    val state: DStream[(String, Int)] = res.updateStateByKey(updateFunction _)

    state.print()

    ssc.start()
    ssc.awaitTermination()

  }

  def updateFunction(newValues: Seq[Int], oldValues: Option[Int]): Option[Int] = {
    val newCount = newValues.sum
    val oldCount = oldValues.getOrElse(0)
    Some(newCount + oldCount)
  }

}
