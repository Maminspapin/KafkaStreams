package streams;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import config.AppConfig;
import model.CommonKey;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import processors.ScenarioCashProcessor;
import transformers.JoinVisitScenarioTransformer;
import transformers.EventKeyTransformer;

import java.util.Properties;

import static config.Topics.MATOMO_LOG_LINK_VISIT_ACTION;
import static config.Topics.MATOMO_SCENARIOS_DIRECTORY;

public class EventStream {

    public static KafkaStreams newStream() {

        try {
            Properties properties = AppConfig.getProperties("EventStream");
            Serde<String> stringSerde = Serdes.String();

            StreamsBuilder builder = new StreamsBuilder();

            StoreBuilder<KeyValueStore<String, String>> store =
                    Stores.keyValueStoreBuilder(
                            Stores.inMemoryKeyValueStore("scenarioStore"),
                            stringSerde,
                            stringSerde
                    );
            builder.addStateStore(store);

            KStream<GenericRecord, GenericRecord> resourceScenarioStream = builder.stream(MATOMO_SCENARIOS_DIRECTORY.topicName());
            KStream<GenericRecord, String> resultScenarioStream = resourceScenarioStream.mapValues(record -> record.get("after").toString());

            resultScenarioStream.print(Printed.toSysOut());

            resultScenarioStream.process(() -> new ScenarioCashProcessor("scenarioStore"), "scenarioStore");

            KStream<GenericRecord, GenericRecord> resourceLinkVisitActionStream = builder.stream(MATOMO_LOG_LINK_VISIT_ACTION.topicName());
            KStream<GenericRecord, String> resultLinkVisitActionStream = resourceLinkVisitActionStream.mapValues(record -> record.get("after").toString());

            KStream<String, String> toJoinLinkVisitActionStream = resultLinkVisitActionStream.selectKey((key, value) -> {

                CommonKey commonKey = new CommonKey();
                commonKey.setAction_id(new Gson().fromJson(value, JsonObject.class).get("action_id").getAsInt());
                commonKey.setCategory_id(new Gson().fromJson(value, JsonObject.class).get("category_id").getAsInt());

                return commonKey.toString();
            });

            toJoinLinkVisitActionStream
                    .transform(() -> new JoinVisitScenarioTransformer("scenarioStore"), "scenarioStore")
                    .transform(EventKeyTransformer::new)
                    .to("events", Produced.with(stringSerde, stringSerde));

            Topology topology = builder.build();

            return new KafkaStreams(topology, properties);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return null;
        }
    }
}