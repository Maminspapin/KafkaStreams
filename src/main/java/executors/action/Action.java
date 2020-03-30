package executors.action;

import executors.executors.eventCashExecutors.EventCash_1100_Executor;
import executors.executors.eventCashExecutors.EventCash_11_Executor;
import executors.executors.eventCashExecutors.EventCash_1300_Executor;
import executors.executors.scenarioExecutors.Scenario_1100_Executor;
import executors.executors.scenarioExecutors.Scenario_11_Executor;
import executors.executors.scenarioExecutors.Scenario_1300_Executor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;

public enum Action {

    SCENARIO_11(new Scenario_11_Executor(), new EventCash_11_Executor()),
    SCENARIO_1100(new Scenario_1100_Executor(), new EventCash_1100_Executor()),
    SCENARIO_1300(new Scenario_1300_Executor(), new EventCash_1300_Executor());

    ScenarioExecutor scenarioExecutor;
    EventCashExecutor eventCashExecutor;

    Action(ScenarioExecutor executor, EventCashExecutor eventCashExecutor) {
        this.scenarioExecutor = executor;
        this.eventCashExecutor = eventCashExecutor;
    }

    public void executeScenario(KeyValue<String, String> kv, KeyValueStore<String, String> store) {
        scenarioExecutor.executeScenario(kv, store);
    }

    public void executeEventCash(Object key, Object value, KeyValueStore<String, String> eventStore) {
        eventCashExecutor.executeEventCash(key, value, eventStore);
    }

}
