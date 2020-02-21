package com.zhc.spark.kafka_test;

/**
 * Kafka Java API测试
 */
public class KafkaClientApp {
    public static void main(String[] args) {

        new KafkaProducer(KafkaProperties.TOPIC).start();

        new KafkaConsumer(KafkaProperties.TOPIC).start();
    }
}
