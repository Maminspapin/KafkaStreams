package executors.executors.scenarioExecutors;

import executors.actions.ScenarioExecutor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;

public class Scenario_20031_Executor implements ScenarioExecutor {

    @Override
    public void executeScenario(KeyValue<String, String> kv, KeyValueStore<String, String> eventStore) {
        //
    }
}
