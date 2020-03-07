package executors;

import executors.scenarioExecutors.Scenario_1_Executor;
import executors.scenarioExecutors.Scenario_2_Executor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;

public enum Action {

    SCENARIO_1(new Scenario_1_Executor()),
    SCENARIO_2(new Scenario_2_Executor());

    ScenarioExecutor executor;

    Action(ScenarioExecutor executor) {
        this.executor = executor;
    }

    public void execute(KeyValue<String, String> kv, KeyValueStore<String, String> store) {
        executor.execute(kv, store);
    }

}
