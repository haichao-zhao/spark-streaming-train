package com.zhc.spark

import java.sql.{Connection, ResultSet}

import com.zhc.spark.utils.MysqlPoolUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ForeachRDDApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("ForeachRDDApp")

    val ssc = new StreamingContext(conf, Seconds(5))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 6788)
    lines.print()
    val res: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    res.print()

    res.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {

        val conn: Connection = MysqlPoolUtil.getConnection

        partitionOfRecords.foreach(record => {

          val query_sql = "select word,cnt from wordcount where word = '" + record._1 + "'"

          val resultSet: ResultSet = conn.createStatement().executeQuery(query_sql)
          if (!resultSet.next()) {

            val insert_sql = "insert into wordcount(word, cnt) values('" + record._1 + "'," + record._2 + ")"
            conn.createStatement().execute(insert_sql)

          } else {
            val old_cnt = resultSet.getInt("cnt")
            val cnt = record._2 + old_cnt

            val update_sql = "UPDATE wordcount SET cnt= " + cnt + " WHERE word='" + record._1 + "'"

            conn.createStatement().execute(update_sql)
          }
        })
        MysqlPoolUtil.returnConnection(conn)
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }


}
