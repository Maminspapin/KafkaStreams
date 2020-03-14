package executors.transformers;

import com.google.gson.JsonObject;
import executors.processors.ScenarioCashProcessor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Utils;

import java.time.Duration;

public class JoinVisitScenarioTransformer implements Transformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(JoinVisitScenarioTransformer.class);

    private ProcessorContext context;
    private String scenarioStoreName;
    private KeyValueStore<String, String> scenarioStore;

    public JoinVisitScenarioTransformer(String scenarioStoreName) {
        this.scenarioStoreName = scenarioStoreName;
    }

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        scenarioStore = (KeyValueStore) processorContext.getStateStore(scenarioStoreName);

        //this.context.schedule(Duration.ofSeconds(15), PunctuationType.WALL_CLOCK_TIME, schedule -> scenarioStore.all().forEachRemaining(kv -> LOGGER.info("Join. Key: " + kv.key + ", Value: " + kv.value)));
    }

    @Override
    public Object transform(Object key, Object value) {

        String result = value.toString();
        String scenario = scenarioStore.get(key.toString());

        if (scenario != null) {

            JsonObject scenarioJson = Utils.getJsonObject(scenario);
            int scenario_id = scenarioJson.get("id").getAsInt();
            String description = scenarioJson.get("description").getAsString();

            JsonObject resultJson = Utils.getJsonObject(result);
            resultJson.addProperty("scenario_id", scenario_id);
            resultJson.addProperty("description", description);

            return new KeyValue<>(key.toString(), resultJson.toString());
        } else {
            return new KeyValue<>(key.toString(), result);
        }
    }

    @Override
    public void close() {

    }
}
