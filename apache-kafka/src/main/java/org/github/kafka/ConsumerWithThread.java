package org.github.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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

public class ConsumerWithThread {
    static Logger logger = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) throws InterruptedException {

        String groupId = "my-fifth-app";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName() );
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("first_topic"));

        CountDownLatch countDownLatch = new CountDownLatch(1);

        Thread thread = new Thread(()-> new ConsumerThread(countDownLatch, consumer));
        thread.start();
        countDownLatch.await();
    }

    public static class ConsumerThread implements Runnable {

        public CountDownLatch latch;
        public KafkaConsumer<String, String> consumer;

        public ConsumerThread(CountDownLatch countDownLatch, KafkaConsumer<String, String> kafkaConsumer ) {
            this.latch = countDownLatch;
            this.consumer = kafkaConsumer;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (var record : records) {
                        logger.info("RECORD: " + record.value());
                        logger.info("PARTITION " +  record.key());
                    }
                }
            } catch (WakeupException e) {
                logger.error("WAKEUP EXCEPTION : " + e.getMessage());
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }
}
