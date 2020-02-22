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
import processors.CashProcessor;
import processors.FilterProcessor;
import utils.Config;

import java.util.Properties;

import static utils.Topics.EVENTS;

public class ResultStream {

    public static KafkaStreams newStream() {

        Properties properties = Config.getProperties("ResultStream");

        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, String>> store =
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("inmemory"),
                        stringSerde,
                        stringSerde
                );
        builder.addStateStore(store);

        KStream<String, String> resourceEventStream = builder.stream(EVENTS.topicName(), Consumed.with(stringSerde, stringSerde));

        resourceEventStream.process(() -> new CashProcessor("inmemory"), "inmemory");
        resourceEventStream.process(() -> new FilterProcessor("inmemory"), "inmemory");

        Topology topology = builder.build();

        return new KafkaStreams(topology, properties);

    }
}
