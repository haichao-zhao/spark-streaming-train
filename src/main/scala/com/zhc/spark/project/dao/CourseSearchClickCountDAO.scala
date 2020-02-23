package com.zhc.spark.project.dao

import java.nio.ByteBuffer

import com.zhc.spark.project.domain.CourseSearchClickCount
import com.zhc.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get

import scala.collection.mutable.ListBuffer

/**
  * 实战课程搜索访问量数据层
  */
object CourseSearchClickCountDAO {

  val tableName = "imooc_course_search_clickcount"
  val cf = "info"
  val qualifer = "click_count"


  /**
    * 保存数据到HBase
    *
    * @param list CourseSearchClickCount集合
    */
  def save(list: ListBuffer[CourseSearchClickCount]): Unit = {

    val table = HBaseUtils.getInstance().getTable(tableName);

    for (ele <- list) {
      table.incrementColumnValue(
        ele.day_search_coursc.getBytes(),
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

}
