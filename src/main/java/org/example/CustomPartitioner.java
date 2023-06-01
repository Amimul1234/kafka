package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class CustomPartitioner {
    public static void produceMessage() {
        String topicName = "SensorTopic";

        Properties properties = new Properties();

        properties.put("bootstrap.servers", "localhost:9092, localhost:9093");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("partitioner.class", "SensorPartitioner");
        properties.put("speed.sensor.name", "TSS");

        Producer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>(topicName, "SSP" + i, "500" + i));
        }

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>(topicName, "TSS", "500" + i));
        }

        producer.close();
    }
}
