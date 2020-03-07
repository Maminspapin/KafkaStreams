package executors.transformers;

import com.google.gson.JsonObject;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import utils.Utils;

public class JoinVisitScenarioTransformer implements Transformer {

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
    }

    @Override
    public Object transform(Object key, Object value) {

        String result = value.toString();
        String scenario = scenarioStore.get(key.toString());

        if (scenario != null) {

            JsonObject scenarioJson = Utils.getJsonObject(scenario);
            int scenario_id = scenarioJson.get("scenario_id").getAsInt();
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
