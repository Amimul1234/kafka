package org.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SimpleProducer {

    public static void produceMessage() {
        String key = "key1";
        String value = "test-1";
        String topicName = "SimpleProducerTopic";

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

        producer.send(record);
        producer.close();

        System.out.println("Simple producer completed");
    }
}
