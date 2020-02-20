package com.zhc.spark.utils

import java.sql.{Connection, DriverManager}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

/**
  * JDBC 连接池
  */
object MysqlPoolUtil {
  // 数据库驱动类
  private val driverClass: String = "com.mysql.jdbc.Driver"
  // 数据库连接地址
  private val url: String = "jdbc:mysql://localhost:3306/imoocbootscala"
  // 数据库连接用户名
  private val username: String = "root"
  // 数据库连接密码
  private val password: String = "root"

  // 加载数据库驱动
  Class.forName(driverClass)

  // 连接池大小
  val poolSize: Int = 3

  // 连接池 - 同步队列
  private val pool: BlockingQueue[Connection] = new LinkedBlockingQueue[Connection]()

  /**
    * 初始化连接池
    */
  for (i <- 1 to poolSize) {
    MysqlPoolUtil.pool.put(DriverManager.getConnection(url, username, password))
  }


  /**
    * 从连接池中获取一个Connection
    *
    * @return
    */
  def getConnection: Connection = {
    pool.take()
  }


  /**
    * 向连接池归还一个Connection
    *
    * @param conn
    */
  def returnConnection(conn: Connection): Unit = {
    MysqlPoolUtil.pool.put(conn)
  }


  /**
    * 启动守护线程释放资源
    */
  def releaseResource() = {
    val thread = new Thread(new CloseRunnable)
    thread.setDaemon(true)
    thread.start()
  }

  /**
    * 关闭连接池连接资源类
    */
  class CloseRunnable extends Runnable {
    override def run(): Unit = {
      while (MysqlPoolUtil.pool.size > 0) {
        try {
          //          println(s"当前连接池大小: ${DBUtil.pool.size}")
          MysqlPoolUtil.pool.take().close()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }
  }
}