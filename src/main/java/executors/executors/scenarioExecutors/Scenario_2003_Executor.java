package executors.executors.scenarioExecutors;

import client.RecordProducer;
import com.google.gson.JsonObject;
import executors.actions.ScenarioExecutor;
import model.EventKey;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.joda.time.DateTime;
import executors.helper.ExecutorHelper;

import static config.Topics.RESULTS;

public class Scenario_2003_Executor implements ScenarioExecutor {

    @Override
    public void executeScenario(KeyValue<String, String> kv, KeyValueStore<String, String> eventStore) {

        EventKey eventKey = ExecutorHelper.getEventKey(kv.key);
        JsonObject eventValue = ExecutorHelper.getJsonObject(kv.value);

        long server_time = eventValue.get("server_time").getAsLong();
        DateTime targetEventTime = ExecutorHelper.getEventTime(server_time);

        EventKey event_1300_Key = new EventKey(eventKey.getUser_id(), 1300);
        boolean event_1300_Exists = ExecutorHelper.eventExists(event_1300_Key, eventStore); // TODO проверить

        if (ExecutorHelper.isCurrentDay(targetEventTime)) {

            if (event_1300_Exists && ExecutorHelper.eventTimeFits(event_1300_Key, targetEventTime, eventStore)) {

                String event_1300_Value = eventStore.get(event_1300_Key.toString());
                JsonObject event_1300_ValueJson = ExecutorHelper.getJsonObject(event_1300_Value);
                long event_1300_server_time = event_1300_ValueJson.get("server_time").getAsLong();
                int push_period = ExecutorHelper.getPushPeriodFromEvent(eventValue, 1800_00);
                ExecutorHelper.updatePushPeriod(push_period, kv.key, eventValue, eventStore);

                if (ExecutorHelper.isPushTime(event_1300_server_time, push_period)) {
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    eventStore.delete(kv.key);
                    eventStore.delete(event_1300_Key.toString());
                } else {
                    eventStore.delete(kv.key);
                    eventStore.delete(event_1300_Key.toString());
                }
            }
        } else {
            eventStore.delete(kv.key);
            eventStore.delete(event_1300_Key.toString());
        }
    }
}
