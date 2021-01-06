package com.github.gy2108.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

        String bootStrapServer = "127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        String topic = "first_topic";

        for (int i = 0; i < 10; i++) {
            String value = "hello world!!"+ Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            //create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key,value);

            logger.info("Key: " + key);
            //send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new Metadata \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "OffSet: " + recordMetadata.offset() + "\n" +
                                "Time Stamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error Occurred while producing", e);
                    }
                }
            }).get();
        }

        //flush data
        producer.flush();

        //flush and close producer
        producer.close();
    }
}
