package com.github.kibourk.kafka.tuto7;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static final String GROUP_ID = "my-sixth-app";
    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    public static final String AUTO_OFFSET_RESET_CONFIG = "earliest";
    public static final String TOPIC = "first_topic";

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        //dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        //creating the consumer runnable
        Runnable myConsumerRunnable = new ConsumerThread(BOOTSTRAP_SERVERS,
                GROUP_ID,
                TOPIC,
                AUTO_OFFSET_RESET_CONFIG,
                latch);

        Thread myThread = new Thread(myConsumerRunnable);

        //start the thread
        myThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) myConsumerRunnable).shutdown();
            try{
                latch.await();
            }catch(InterruptedException e){
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }
}
