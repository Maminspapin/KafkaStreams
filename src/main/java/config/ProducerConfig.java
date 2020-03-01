package config;

import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerConfig {

    public static Properties getProperties() {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "172.17.188.49:9092");
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);

        return properties;
    }
}
