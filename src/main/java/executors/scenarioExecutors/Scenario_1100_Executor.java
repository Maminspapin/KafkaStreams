package executors.scenarioExecutors;

import executors.ScenarioExecutor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;

public class Scenario_1100_Executor implements ScenarioExecutor {

    @Override
    public void execute(KeyValue<String, String> kv, KeyValueStore<String, String> store) {
        // do something, another logic
    }
}
