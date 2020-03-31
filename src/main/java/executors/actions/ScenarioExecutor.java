package executors.actions;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;

public interface ScenarioExecutor {

    void executeScenario(KeyValue<String, String> kv, KeyValueStore<String, String> store);
}
