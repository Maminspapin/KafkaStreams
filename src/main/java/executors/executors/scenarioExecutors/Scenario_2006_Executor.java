package executors.executors.scenarioExecutors;

import client.RecordProducer;
import com.google.gson.JsonObject;
import executors.actions.ScenarioExecutor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import utils.Utils;

import static config.Topics.RESULTS;

public class Scenario_2006_Executor implements ScenarioExecutor {

    @Override
    public void executeScenario(KeyValue<String, String> kv, KeyValueStore<String, String> store) {

        JsonObject eventValue = Utils.getJsonObject(kv.value);
        RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
        store.delete(kv.key);
    }
}
