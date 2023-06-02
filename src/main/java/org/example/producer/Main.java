package org.example.producer;

public class Main {
    public static void main(String[] args) {
//        SimpleProducer.produceMessage();
//        SynchronousProducer.produceMessage();
        AsynchronousSend.produceMessage();
    }
}