package com.zhc.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * 黑名单过滤功能
  */

object TransformApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformApp")

    val ssc = new StreamingContext(conf, Seconds(5))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 6788)

    //构建黑名单
    val blacks = List("zs", "ls")
    val blackRDD: RDD[(String, Boolean)] = ssc.sparkContext.parallelize(blacks).map(x => (x, true))

    val res: DStream[String] = lines.map(x => (x.split(",")(1), x))
      .transform(rdd => {
        rdd.leftOuterJoin(blackRDD)
          .filter(x => x._2._2.getOrElse(false) != true)
          .map(x => x._2._1)
      })

    res.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
