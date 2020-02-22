package utils;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class Config {

    public static Properties getProperties(String appId) {

        Properties properties = new Properties(); // TODO настройки вынести в отдельный файл?

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        //properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.188.49:9092");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //properties.put("schema.registry.url", "http://172.17.188.49:8081");
        properties.put("schema.registry.url", "http://localhost:8081");

        return properties;
    }
}
