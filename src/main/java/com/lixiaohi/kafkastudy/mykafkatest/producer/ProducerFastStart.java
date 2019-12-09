package com.lixiaohi.kafkastudy.mykafkatest.producer;

import kafka.tools.ConsoleProducer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @author lixiaochi
 * @date 2019/12/4-17:15
 * @message kafka消息生产者
 */
public class ProducerFastStart {
    // kafka集群地址
    private static final String brokerList = "101.200.194.183:9092";

    // 主题名称-之前已经创建好的主题
    private static final String topic = "lixiaochi";

    public static void main(String[] args) {
        Properties properties = new Properties();
        // 设置key序列化器
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");

        // 另外一种写法
        // properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 设置重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG,10);

        // 设置值序列化器
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        // properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // 设置集群地址
        properties.put("bootstrap.servers",brokerList);
        // properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);

        // kafkaProducer 线程安全
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        // 向kafka服务器，发送的消息
        ProducerRecord<String,String> record = new ProducerRecord<>(topic,"kafka-demo-001");

        // 生产者发送消息
        try {
//            Future<RecordMetadata> send = producer.send(record);
//            RecordMetadata recordMetadata = send.get();
//            System.out.println(recordMetadata.topic());
            producer.send(record,new Callback() {
                public void onCompletion(RecordMetadata recordMetadata,Exception exception) {
                    if(exception == null) {
                        System.out.println(recordMetadata.partition()+";"+recordMetadata.offset());
                    }
                }
            });
            System.out.println("lixiaochi");

        } catch(Exception e){
            e.printStackTrace();
        } finally {
            producer.close();
        }

    }
}
