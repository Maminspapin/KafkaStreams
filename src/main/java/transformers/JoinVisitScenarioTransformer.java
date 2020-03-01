package transformers;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

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
            String scenario_id = new Gson().fromJson(scenario, JsonObject.class).get("scenario_id").toString();
            String description = new Gson().fromJson(scenario, JsonObject.class).get("description").toString();
            result = result.replace("}", ", \"scenario_id\": " + scenario_id + ", \"description\": " + description + "}");
            return new KeyValue<>(key.toString(), result);
        } else {
            return new KeyValue<>(null, result);
        }
    }

    @Override
    public void close() {

    }
}
