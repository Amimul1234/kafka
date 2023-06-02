package org.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.dro.Supplier;

import java.util.List;
import java.util.Properties;

public class ManualConsumer {
    public static void main(String[] args) {
        String topicName = "SupplierTopic";
        String groupName = "SupplierTopicGroup";

        Properties properties = new Properties();

        properties.put("bootstrap.servers", "localhost:9092, localhost:9093");
        properties.put("group.id", groupName);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.example.serializer.SupplierDeserializer");
        properties.put("enable.auto.commit", "false");

        KafkaConsumer<String, Supplier> consumer = new KafkaConsumer<>(properties);

        try {
            consumer.subscribe(List.of(topicName));

            while (true) {
                ConsumerRecords<String, Supplier> records = consumer.poll(100);
                for (ConsumerRecord<String, Supplier> record : records) {
                    System.out.println("Supplier id = " + record.value().getID() + " Supplier name: " + record.value().getName());
                }
                consumer.commitAsync();
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            consumer.commitSync();
            consumer.close();
        }
    }
}
