package executors.executors.scenarioExecutors;

import client.RecordProducer;
import com.google.gson.JsonObject;
import executors.actions.ScenarioExecutor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import executors.helper.ExecutorHelper;

import static config.Topics.RESULTS;

public class Scenario_2005_Executor implements ScenarioExecutor {

    @Override
    public void executeScenario(KeyValue<String, String> kv, KeyValueStore<String, String> eventStore) {

        JsonObject eventValue = ExecutorHelper.getJsonObject(kv.value);
        int push_period = ExecutorHelper.getPushPeriodFromEvent(eventValue, 600_00);
        ExecutorHelper.updatePushPeriod(push_period, kv.key, eventValue, eventStore); // TODO здесь и в подобных перевести в if
        long server_time = eventValue.get("server_time").getAsLong();

        if (ExecutorHelper.isPushTime(server_time, push_period)) {
            RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
            eventStore.delete(kv.key);
        }
    }
}

