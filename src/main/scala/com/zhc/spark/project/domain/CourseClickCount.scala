package com.zhc.spark.project.domain

/**
  * 实战课程访问量实体类
  *
  * @param day_coursc  对应的就是HBase中的rowkey，20191111_131
  * @param click_count 对应的20191111_131的访问量
  */
case class CourseClickCount(day_coursc: String, click_count: Long)