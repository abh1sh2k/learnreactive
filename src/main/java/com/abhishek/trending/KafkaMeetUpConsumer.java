package com.abhishek.trending;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaMeetUpConsumer {
    String topic = "meetup-trending-topics";

    public KafkaConsumer<String, String> initialiseConsumer(){

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("partition.assignment.strategy", "range");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        return consumer;
    }

    public static void main(String[] args) {
        KafkaMeetUpConsumer meetupConsumer = new KafkaMeetUpConsumer();
        KafkaConsumer<String, String> consumer = meetupConsumer.initialiseConsumer();
        //consumer.subscribe(Pattern.compile(meetupConsumer.topic));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                {
                    System.out.println("topic = " + record.topic() + " partition = " + record.partition() + " value = " + record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
