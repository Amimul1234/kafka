package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class SynchronousProducer {

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

        try {
            RecordMetadata recordMetadata = producer.send(record).get();
            System.out.println("Message is sent to partition no: " + recordMetadata.partition() + " and offset: " + recordMetadata.offset());
            System.out.println("Synchronous completed with success");
        } catch (Exception exception) {
            exception.printStackTrace();
            System.out.println("SynchronousProducer failed with an exception");
        } finally {
            producer.close();
        }
    }
}
