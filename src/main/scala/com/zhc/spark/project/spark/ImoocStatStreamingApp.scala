package com.zhc.spark.project.spark

import com.zhc.spark.project.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import com.zhc.spark.project.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.zhc.spark.project.utils.DateUtils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * Spark Streaming对接Kafka
  */
object ImoocStatStreamingApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("ImoocStatStreamingApp")

    val ssc = new StreamingContext(conf, Seconds(60))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val topics = Set[String]("kafka_streaming_topic")


    val messages: InputDStream[(String, String)] = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics
    )

    val logs = messages.map(_._2)

    //数据清洗
    val cleanData = logs.map(lines => {
      val infos = lines.split("\t")

      var courseId = 0

      val url = infos(2).split(" ")(1)
      if (url.startsWith("class")) {
        val courseIdHTML = url.split("/")(1)
        courseId = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt
      }

      ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), courseId, infos(3).toInt, infos(4))

    }).filter(clicklog => clicklog.courseId != 0)

    cleanData.print()

    //统计今天到现在为止实战课程的访问量
    //将统计结果写入HBase
    cleanData.map(x => {
      //HBase rowkey设计： 20191111_131
      (x.time.substring(0, 8) + "_" + x.courseId, 1)
    }).reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(partitionRecords => {

          val list = new ListBuffer[CourseClickCount]

          partitionRecords.foreach(pari => {
            list.append(CourseClickCount(pari._1, pari._2))
          })

          CourseClickCountDAO.save(list)

        })
      })

    //统计从搜索引擎过来的今天到现在为止实战课程的访问量
    //将统计结果写入HBase
    cleanData.filter(x => x.referer != "-").map(x => {
      //HBase rowkey设计： 20191111_www.baidu.com_131

      val url = x.referer.split("/")(2)

      (x.time.substring(0, 8) + "_" + url + "_" + x.courseId, 1)
    }).reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(partitionRecords => {

          val list = new ListBuffer[CourseSearchClickCount]

          partitionRecords.foreach(pari => {
            list.append(CourseSearchClickCount(pari._1, pari._2))
          })

          CourseSearchClickCountDAO.save(list)

        })
      })


    ssc.start()
    ssc.awaitTermination()

  }

}
