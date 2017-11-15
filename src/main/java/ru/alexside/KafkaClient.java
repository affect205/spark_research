package ru.alexside;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.Properties;

/**
 * Created by abalyshev on 05.07.17.
 */
public class KafkaClient {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Thread topic1 = new  Thread(() -> {
            KafkaProducer<String, String> producer = new KafkaProducer<>(props);
            for (int i = 0; i < 40000000; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("extdata4", "value-" + Instant.now().toEpochMilli());
                producer.send(record);
            }

            producer.close();
        });
        Thread topic2 = new  Thread(() -> {
            KafkaProducer<String, String> producer = new KafkaProducer<>(props);
            for (int i = 0; i < 40000000; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("extdata3", "value-" + Instant.now().toEpochMilli());
                producer.send(record);
            }

            producer.close();
        });

        topic1.start();
        topic2.start();

        System.out.println("Await the end...");
        topic1.join();
        topic2.join();
    }
}
