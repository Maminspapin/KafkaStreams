package streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import executors.processors.EventCashProcessor;
import executors.processors.ResultFilterProcessor;
import config.AppConfig;

import java.util.Properties;

import static config.Topics.EVENTS;

public class ResultStream {

    public static KafkaStreams newStream() {

        Properties properties = AppConfig.getStreamProperties("ResultStream");
        Serde<String> stringSerde = Serdes.String();
        String storeName = "visitStore";

        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, String>> store =
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(storeName),
                        stringSerde,
                        stringSerde
                );
        builder.addStateStore(store);

        KStream<String, String> resourceEventStream = builder.stream(EVENTS.topicName(), Consumed.with(stringSerde, stringSerde));

        resourceEventStream.process(() -> new EventCashProcessor(storeName), storeName);
        resourceEventStream.process(() -> new ResultFilterProcessor(storeName), storeName);

        Topology topology = builder.build();

        return new KafkaStreams(topology, properties);

    }
}
