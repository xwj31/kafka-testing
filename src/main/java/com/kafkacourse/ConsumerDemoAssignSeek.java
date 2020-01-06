package com.kafkacourse;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {

        String bootStrapServers = " 127.0.0.1:9094";
        String groupId = "my-seventh-application";
        String topic = "first_topic";

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> kafkaConsumer =
                new KafkaConsumer<String, String>(properties);

        //subscribe consumer to topic
        //kafkaConsumer.subscribe(Collections.singleton("first_topic"));

        //assign and seek to replay data or find a specific message
        TopicPartition topicPartitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        kafkaConsumer.assign(Arrays.asList(topicPartitionToReadFrom));

        //seek
        kafkaConsumer.seek(topicPartitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        while (keepOnReading) {
            ConsumerRecords<String, String> consumerRecords =
                    kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: consumerRecords) {
                numberOfMessagesReadSoFar += 1;
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + " Offset: " + record.offset());

                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }
        logger.info("exiting the application");
    }
}
