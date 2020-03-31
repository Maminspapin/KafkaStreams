package executors.actions;

import executors.executors.eventCashExecutors.*;
import executors.executors.scenarioExecutors.*;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;

public enum Action {

    SCENARIO_11(new Scenario_11_Executor(), new EventCash_11_Executor()),
    SCENARIO_21(new Scenario_21_Executor(), new EventCash_21_Executor()),
    //SCENARIO_1100(new Scenario_1100_Executor(), new EventCash_1100_Executor()),
    SCENARIO_1300(new Scenario_1300_Executor(), new EventCash_1300_Executor()),
    SCENARIO_1500(new Scenario_1500_Executor(), new EventCash_1500_Executor()),
    SCENARIO_2001(new Scenario_2001_Executor(), new EventCash_2001_Executor()),
    SCENARIO_2002(new Scenario_2002_Executor(), new EventCash_2002_Executor()),
    SCENARIO_2003(new Scenario_2003_Executor(), new EventCash_2003_Executor()),
    SCENARIO_2004(new Scenario_2004_Executor(), new EventCash_2004_Executor()),
    SCENARIO_2005(new Scenario_2005_Executor(), new EventCash_2005_Executor()),
    SCENARIO_2006(new Scenario_2006_Executor(), new EventCash_2006_Executor()),
    SCENARIO_2007(new Scenario_2007_Executor(), new EventCash_2007_Executor()),
    SCENARIO_2008(new Scenario_2008_Executor(), new EventCash_2008_Executor()),
    SCENARIO_2009(new Scenario_2009_Executor(), new EventCash_2009_Executor()),
    SCENARIO_2010(new Scenario_2010_Executor(), new EventCash_2010_Executor());

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
