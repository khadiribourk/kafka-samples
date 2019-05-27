package com.github.kibourk.kafka.tuto6;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerInsideConsumerGroupDemo {
    //reset the group id to read from the beginning
    //rebalancing : start two consumers to see  (AbstractCoordinator  and  ConsumerCoordinator):each consumer instance assign distintcs partitions
    public static final String GROUP_ID = "my-fifth-app";
    public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    public static final String AUTO_OFFSET_RESET_CONFIG = "earliest";

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ConsumerInsideConsumerGroupDemo.class.getName());
        //create the consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_CONFIG);

        //Create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe to our topic
        consumer.subscribe(Arrays.asList("first_topic"));

        //poll for the new data
        while(true){
            ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord record:records){
                logger.info("Key: " + record.key() + ", Value " + record.value());
                logger.info("topic: " + record.topic() + ", partition: " + record.partition() + ", offset:" + record.offset());
            }
        }

    }
}
