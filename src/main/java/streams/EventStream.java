package streams;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import config.AppConfig;
import executors.processors.ScenarioCashProcessor;
import executors.transformers.EventKeyTransformer;
import executors.transformers.JoinVisitScenarioTransformer;
import model.JoinKey;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import utils.Utils;

import java.util.Properties;

import static config.Topics.MATOMO_LOG_LINK_VISIT_ACTION;
import static config.Topics.MATOMO_SCENARIOS_DIRECTORY;

public class EventStream {

    public static KafkaStreams newStream() {


        Properties properties = AppConfig.getStreamProperties("EventStream");
        Serde<String> stringSerde = Serdes.String();
        String storeName = "scenarioStore";

        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, String>> store =
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(storeName),
                        stringSerde,
                        stringSerde
                );
        builder.addStateStore(store);

        KStream<GenericRecord, GenericRecord> resourceScenarioStream = builder.stream(MATOMO_SCENARIOS_DIRECTORY.topicName());
        KStream<GenericRecord, String> resultScenarioStream = resourceScenarioStream.mapValues(record -> record.get("after").toString());

        resultScenarioStream.process(() -> new ScenarioCashProcessor(storeName), storeName);

        KStream<GenericRecord, GenericRecord> resourceLinkVisitActionStream = builder.stream(MATOMO_LOG_LINK_VISIT_ACTION.topicName());
        KStream<GenericRecord, String> resultLinkVisitActionStream = resourceLinkVisitActionStream.mapValues(record -> {

            JsonObject visitValueJson = Utils.getJsonObject(record.get("after").toString());
            visitValueJson.addProperty("push_period", 0);

            return new Gson().toJson(visitValueJson);
        });

        KStream<String, String> toJoinLinkVisitActionStream = resultLinkVisitActionStream.selectKey((key, value) -> {

            JoinKey joinKey = new JoinKey();
            JsonObject visitValueJson = Utils.getJsonObject(value);
            joinKey.setAction_id(visitValueJson.get("action_id").getAsInt());
            joinKey.setCategory_id(visitValueJson.get("category_id").getAsInt());

            return joinKey.toString();
        });

        toJoinLinkVisitActionStream
                .transform(() -> new JoinVisitScenarioTransformer(storeName), storeName)
                .transform(EventKeyTransformer::new)
                .to("events", Produced.with(stringSerde, stringSerde));

        Topology topology = builder.build();

        return new KafkaStreams(topology, properties);
    }
}