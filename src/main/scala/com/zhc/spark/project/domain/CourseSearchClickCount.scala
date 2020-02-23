package com.zhc.spark.project.domain

/**
  * 实战课程搜索访问量实体类
  *
  * @param day_search_coursc  对应的就是HBase中的rowkey，20191111_url_131
  * @param click_count 对应的20191111_url_131的访问量
  */
case class CourseSearchClickCount(day_search_coursc: String, click_count: Long)