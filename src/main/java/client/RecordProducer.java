package client;

import config.AppConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordProducer.class);

    public static void sendRecord(String topic, String key, String value) {

        try (Producer<String, String> resultsProducer = new KafkaProducer<>(AppConfig.getProducerProperties())) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            Callback callback = (metadata, exception) -> {
                if (exception != null) {
                    LOGGER.error("Error while sending message to topic: " + topic + ". ", exception);
                }
            };
            resultsProducer.send(record, callback);
        }
    }
}
