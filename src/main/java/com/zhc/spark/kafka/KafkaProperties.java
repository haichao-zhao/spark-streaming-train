package com.zhc.spark.kafka;

/**
 * Kafka常用配置文件属性
 */
public class KafkaProperties {
    //zookeeper 地址
    public static final String ZK = "localhost:2181";

    //topic名称
    public static final String TOPIC = "hello_topic";

    //broker_list 监听地址
    public static final String BROKER_LIST = "localhost:9092";

    //group id
    public static final String GROUP_ID = "test_group_1";
}
