package com.zhc.spark.project.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HBase操作工具类
 */
public class HBaseUtils {
    HBaseAdmin admin = null;
    Configuration conf = null;

    /**
     * 私有构造方法
     */
    private HBaseUtils() {
        conf = new Configuration();

        conf.set("hbase.zookeeper.quorum", "localhost:2181");
        conf.set("hbase.rootdir", "hdfs://localhost:8020/hbase");

        try {
            admin = new HBaseAdmin(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtils instance = null;

    public static synchronized HBaseUtils getInstance() {
        if (null == instance) {
            instance = new HBaseUtils();
        }
        return instance;
    }

    /**
     * 根据表名获取HTable实例
     *
     * @param tableName
     * @return
     */
    public HTable getTable(String tableName) {

        HTable table = null;

        try {
            table = new HTable(conf, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return table;
    }

    /**
     * 添加一条记录到HBase表
     *
     * @param tableName HBase表名
     * @param rowkey    HBase表的rowkey
     * @param cf        HBase表的columnfamily
     * @param column    HBase表的列
     * @param value     写入HBase表的值
     */
    public void put(String tableName, String rowkey, String cf, String column, Long value) {

        HTable table = getTable(tableName);

        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));

        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
//        HTable table = HBaseUtils.getInstance().getTable("imooc_course_clickcount");
//
//        System.out.println(table.getName().getNameAsString());

        String tableName = "imooc_course_clickcount";
        String rowkey = "20191111_131";
        String cf = "info";
        String column = "click_count";
        Long value = 2L;

        HBaseUtils.getInstance().put(tableName, rowkey, cf, column, value);

    }

}
