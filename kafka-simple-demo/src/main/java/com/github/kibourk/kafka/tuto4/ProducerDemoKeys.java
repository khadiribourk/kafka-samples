package com.github.kibourk.kafka.tuto4;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    //always the same key goes to the same partition
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger= LoggerFactory.getLogger(ProducerDemoKeys.class);
        String bootstrapServers="127.0.0.1:9092";
        // create producer properties (Producer configs https://kafka.apache.org/documentation/#configuration)
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String,String> producer=new KafkaProducer<String,String>(properties);
        final String topic = "first_topic";
        for(int i=0;i<10;i++) {
            //create a producer record
            final String value = "hello world from java producer with callback " + Integer.toString(i);
            final String key="id_"+Integer.toString(i);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key, value);
            logger.info("key"+key);
            //send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //to check if record is successfully sent or an exception is thrown
                    if (e == null) {
                        //on success
                        logger.info("received new metadata. \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "Offset:" + recordMetadata.timestamp());
                    } else {
                        //on fail
                        logger.error("Error while producing", e);
                    }
                }
            }).get();//block the send() to make synchronous =>just for the demo, don't use this in production
        }
        //flush the producer
        producer.flush();

        //close the producer
        producer.close();
    }
}
