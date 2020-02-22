package client;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {

    private static Properties getProperties() {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put("acks", "1");
//        properties.put("retries", "3");
//        properties.put("compression.type", "snappy");

        return properties;
    }

    public static void sendRecord(String topic, String key, String value) {
        try (org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(getProperties())) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            Callback callback = (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                }
            };
            producer.send(record, callback);
        }
    }
}
