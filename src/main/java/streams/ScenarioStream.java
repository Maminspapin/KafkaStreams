package streams;

import com.google.gson.JsonObject;
import config.AppConfig;
import executors.processors.ScenarioCashProcessor;
import model.JoinKey;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import executors.helper.ExecutorHelper;

import java.util.Properties;

import static config.Topics.MATOMO_SCENARIOS_DETAIL;

public class ScenarioStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScenarioStream.class);

    public static KafkaStreams newStream() {

        Properties properties = AppConfig.getStreamProperties("ScenarioStream");
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

        KStream<GenericRecord, GenericRecord> resourceScenarioStream = builder.stream(MATOMO_SCENARIOS_DETAIL.topicName());
        KStream<GenericRecord, String> resultScenarioStream = resourceScenarioStream.mapValues(record -> record.get("after").toString());

        KStream<String, String> toJoinScenarioStream = resultScenarioStream.selectKey((key, value) -> {

            JoinKey joinKey = new JoinKey();
            JsonObject visitValueJson = ExecutorHelper.getJsonObject(value);

            int idaction_event_action = 0;
            int idaction_event_category = 0;

            try {
                idaction_event_action = visitValueJson.get("action_id").getAsInt();
            } catch (NullPointerException e) {
                LOGGER.info("No action_id parameter detected...");
            }

            try {
                idaction_event_category = visitValueJson.get("category_id").getAsInt();
            } catch (NullPointerException e) {
                LOGGER.info("No category_id parameter detected...");
            }

            joinKey.setAction_id(idaction_event_action);
            joinKey.setCategory_id(idaction_event_category);

            return joinKey.toString();
        });

        resourceScenarioStream.print(Printed.toSysOut());
        toJoinScenarioStream.process(() -> new ScenarioCashProcessor(storeName), storeName);

        Topology topology = builder.build();

        return new KafkaStreams(topology, properties);
    }
}