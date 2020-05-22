package org.github.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {

    static Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

    public static void main(String[] args) {
        //create producer props
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        for (int i = 0; i < 10; i++) {
            // create producer record
            String key = "id_" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic",key, "some" + i);
            // create producer
            KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
            //send data

            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("MESSAGE IS SENT SUCCESSFULLY");
                    logger.info(recordMetadata.toString());
                    return;
                }
                logger.error("SOMETHING GETS WRONG " + e.getMessage());
            });
            producer.flush();
            producer.close();
        }

    }
}
