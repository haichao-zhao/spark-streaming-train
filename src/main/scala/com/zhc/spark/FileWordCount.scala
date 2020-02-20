package com.zhc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream}

object FileWordCount {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("FileWordCount")

    val ssc = new StreamingContext(conf,Seconds(5))

    val lines= ssc.textFileStream("file:///Users/zhaohaichao/data/out/")

    val res: DStream[(String, Int)] = lines.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)

    res.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
