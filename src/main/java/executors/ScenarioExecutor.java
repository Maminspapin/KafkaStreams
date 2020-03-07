package executors;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;

public interface ScenarioExecutor {

    void execute(KeyValue<String, String> kv, KeyValueStore<String, String> store);
}
