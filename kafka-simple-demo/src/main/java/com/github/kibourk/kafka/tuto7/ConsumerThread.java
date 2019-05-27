package com.github.kibourk.kafka.tuto7;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThread implements Runnable {

    private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());
    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;

    public ConsumerThread(String bootstrapServers,
                          String groupId,
                          String topic,
                          String autoOffsetReset,
                          CountDownLatch latch) {
        this.latch = latch;
        //create the consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

        //Create the consumer
        consumer = new KafkaConsumer<String, String>(properties);
        //subscribe to our topic
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        //poll for the new data
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord record : records) {
                    logger.info("Key: " + record.key() + ", Value " + record.value());
                    logger.info("topic: " + record.topic() + ", partition: " + record.partition() + ", offset:" + record.offset());
                }
            }
        } catch (WakeupException e) {
            logger.error("Received shutdown signal!");
        } finally {
            consumer.close();
            //tell main that we are done with the consumer
            latch.countDown();
        }

    }

    public void shutdown() {
        //the wakeup method is a special method to iterrupt consumer.poll()
        consumer.wakeup();
    }
}
