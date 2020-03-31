package executors.executors.scenarioExecutors;

import executors.actions.ScenarioExecutor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Scenario_1500_Executor implements ScenarioExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Scenario_1500_Executor.class);

    @Override
    public void executeScenario(KeyValue<String, String> kv, KeyValueStore<String, String> store) {
        // nothing to do
    }
}
