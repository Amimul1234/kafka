package org.example.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MyProducerCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            exception.printStackTrace();
            System.out.println("Asynchronous failed with an exception");
        } else {
            System.out.println("Asynchronous called successfully");
        }
    }
}
