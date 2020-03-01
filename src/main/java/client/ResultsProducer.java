package client;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import config.ProducerConfig;

public class ResultsProducer {

    public static void sendRecord(String topic, String key, String value) {

        try (Producer<String, String> resultsProducer = new KafkaProducer<>(ProducerConfig.getProperties())) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            Callback callback = (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                }
            };
            resultsProducer.send(record, callback);
        }
    }
}
