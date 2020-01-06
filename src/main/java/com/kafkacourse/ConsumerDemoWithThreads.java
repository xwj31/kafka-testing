package com.kafkacourse;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    private ConsumerDemoWithThreads() {

    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

        String bootStrapServers = " 127.0.0.1:9094";
        String groupId = "my-sixth-application";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);

        logger.info("creating consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(
                latch,
                bootStrapServers,
                groupId,
                topic
        );

        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application was interrupted", e);
        } finally {
            logger.info("application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> kafkaConsumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

        public ConsumerRunnable(CountDownLatch latch,
                                String bootStrapServers,
                                String groupId,
                                String topic) {

            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //create consumer
            kafkaConsumer = new KafkaConsumer<String, String>(properties);

            //subscribe consumer to topic
            kafkaConsumer.subscribe(Collections.singleton(topic));
        }


        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords =
                            kafkaConsumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("shutdown received");
            } finally {
                kafkaConsumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            //interrupts kafkaConsumer.poll(), throws wake up exception
            kafkaConsumer.wakeup();
        }
    }
}
