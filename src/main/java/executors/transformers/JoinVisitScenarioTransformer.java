package executors.transformers;

import com.google.gson.JsonObject;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import executors.helper.ExecutorHelper;

public class JoinVisitScenarioTransformer implements Transformer {

    private ProcessorContext context;
    private String scenarioStoreName;
    private KeyValueStore<String, String> scenarioStore;
    private int count = 0;

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

            JsonObject scenarioJson = ExecutorHelper.getJsonObject(scenario);
            int scenario_directory_id = scenarioJson.get("scenarios_directory_id").getAsInt();
            int scenario_id = scenarioJson.get("id").getAsInt();
            String description = scenarioJson.get("description").getAsString();

            JsonObject resultJson = ExecutorHelper.getJsonObject(result);
            resultJson.addProperty("scenarios_directory_id", scenario_directory_id);
            resultJson.addProperty("scenario_id", scenario_id);
            resultJson.addProperty("description", description);

            if (scenario_id == 11) {
                resultJson.addProperty("count", ++count);
            }

            return new KeyValue<>(key.toString(), resultJson.toString());
        } else {
            return new KeyValue<>(key.toString(), result);
        }
    }

    @Override
    public void close() {

    }
}
