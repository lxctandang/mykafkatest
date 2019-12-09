package com.lixiaohi.kafkastudy.mykafkatest.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * @author lixiaochi
 * @date 2019/12/4-17:41
 * @message kafak 消息消费者
 */
public class ConsumerFastStart {
    // kafka集群地址
    private static final String brokerList = "101.200.194.183:9092";

    // 主题名称-之前已经创建好的主题
    private static final String topic = "lixiaochi";

    // 消费组
    private static final String groupId = "group.demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        // 设置key的反序列化器
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");


        // 另外一种写法  可以使用提供的配置类中的配置信息。
        // properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 设置值反序列化器
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        // properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 设置集群地址
        properties.put("bootstrap.servers",brokerList);
        // properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);

        properties.put("group.id",groupId);
        // properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
        }
    }
}
