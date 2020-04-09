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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import executors.helper.ExecutorHelper;

import java.util.Properties;

import static config.Topics.MATOMO_LOG_LINK_VISIT_ACTION;
import static config.Topics.MATOMO_SCENARIOS_DETAIL;

public class EventStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventStream.class);

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


        KStream<GenericRecord, GenericRecord> resourceScenarioStream = builder.stream(MATOMO_SCENARIOS_DETAIL.topicName(), Consumed.with(Topology.AutoOffsetReset.EARLIEST));
        KStream<GenericRecord, String> resultScenarioStream = resourceScenarioStream.mapValues(record -> record.get("after").toString());

        KStream<String, String> toJoinScenarioStream = resultScenarioStream.selectKey((key, value) -> {

            JoinKey joinKey = new JoinKey();
            JsonObject visitValueJson = ExecutorHelper.getJsonObject(value);

            int idaction_event_action = 0;
            int idaction_event_category = 0;

            try {
                idaction_event_action = visitValueJson.get("action_id").getAsInt();
            } catch (NullPointerException e) {
                LOGGER.debug("No action_id parameter detected...");
            }

            try {
                idaction_event_category = visitValueJson.get("category_id").getAsInt();
            } catch (NullPointerException e) {
                LOGGER.debug("No category_id parameter detected...");
            }

            joinKey.setAction_id(idaction_event_action);
            joinKey.setCategory_id(idaction_event_category);

            return joinKey.toString();
        });

        resourceScenarioStream.print(Printed.toSysOut());
        toJoinScenarioStream.process(() -> new ScenarioCashProcessor(storeName), storeName);


        KStream<GenericRecord, GenericRecord> resourceLinkVisitActionStream = builder.stream(MATOMO_LOG_LINK_VISIT_ACTION.topicName());

        KStream<GenericRecord, String> resultLinkVisitActionStream = resourceLinkVisitActionStream.mapValues(record -> {

            try {
                JsonObject visitValueJson = ExecutorHelper.getJsonObject(record.get("after").toString());
                visitValueJson.addProperty("push_period", 0);
                return new Gson().toJson(visitValueJson);
            } catch (Exception e) {
                LOGGER.info("Error while getting data from " + MATOMO_LOG_LINK_VISIT_ACTION.topicName() + " topic. ", e);
                return "UselessMessage";
            }
        });


        KStream<String, String> toJoinLinkVisitActionStream = resultLinkVisitActionStream.selectKey((key, value) -> {

            JoinKey joinKey = new JoinKey();
            JsonObject visitValueJson = ExecutorHelper.getJsonObject(value);

            int idaction_event_action = 0;
            int idaction_event_category = 0;

            try {
                idaction_event_action = visitValueJson.get("idaction_event_action").getAsInt();
            } catch (NullPointerException e) {
                LOGGER.debug("No idaction_event_action parameter detected...");
            }

            try {
                idaction_event_category = visitValueJson.get("idaction_event_category").getAsInt();
            } catch (NullPointerException e) {
                LOGGER.debug("No idaction_event_category parameter detected...");
            }

            joinKey.setAction_id(idaction_event_action);
            joinKey.setCategory_id(idaction_event_category);

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