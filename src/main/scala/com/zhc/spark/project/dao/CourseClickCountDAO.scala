package com.zhc.spark.project.dao

import java.nio.ByteBuffer

import com.zhc.spark.project.domain.CourseClickCount
import com.zhc.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get

import scala.collection.mutable.ListBuffer

/**
  * 实战课程访问量数据层
  */
object CourseClickCountDAO {

  val tableName = "imooc_course_clickcount"
  val cf = "info"
  val qualifer = "click_count"


  /**
    * 保存数据到HBase
    *
    * @param list CourseClickCount集合
    */
  def save(list: ListBuffer[CourseClickCount]): Unit = {

    val table = HBaseUtils.getInstance().getTable(tableName);

    for (ele <- list) {
      table.incrementColumnValue(
        ele.day_coursc.getBytes(),
        cf.getBytes(),
        qualifer.getBytes(),
        ele.click_count)
    }


  }


  /**
    * 根据rowkey查询值
    */
  def count(day_course: String): Long = {

    val table = HBaseUtils.getInstance().getTable(tableName);

    val get = new Get(day_course.getBytes())

    val bytes: Array[Byte] = table.get(get).getValue(cf.getBytes(), qualifer.getBytes())

    if (bytes == null) {
      0L
    } else {
      ByteBuffer.wrap(bytes).getLong()
    }
  }


  def main(args: Array[String]): Unit = {

    val list = new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("20191111_131",2L))

    save(list)

    println(count("20191111_131"))
  }

}
