package executors.executors.scenarioExecutors;

import client.RecordProducer;
import com.google.gson.JsonObject;
import executors.actions.ScenarioExecutor;
import model.EventKey;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import executors.helper.ExecutorHelper;

import static config.Topics.RESULTS;

public class Scenario_2004_Executor implements ScenarioExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Scenario_2004_Executor.class);

    @Override
    public void executeScenario(KeyValue<String, String> kv, KeyValueStore<String, String> eventStore) {

        EventKey eventKey = ExecutorHelper.getEventKey(kv.key);
        JsonObject eventValue = ExecutorHelper.getJsonObject(kv.value);

        long server_time = eventValue.get("server_time").getAsLong();
        DateTime targetEventTime = ExecutorHelper.getEventTime(server_time);

        EventKey event_1300_Key = new EventKey(eventKey.getUser_id(), 1300);
        boolean event_1300_Exists = ExecutorHelper.eventExists(event_1300_Key, eventStore); // TODO проверить

        EventKey event_20031_Key = new EventKey(eventKey.getUser_id(), 20031);
        boolean event_20031_Exists = ExecutorHelper.eventExists(event_20031_Key, eventStore);

        EventKey event_20032_Key = new EventKey(eventKey.getUser_id(), 20032);
        boolean event_20032_Exists = ExecutorHelper.eventExists(event_20032_Key, eventStore);

        if (ExecutorHelper.isCurrentDay(targetEventTime)) {

            if (event_1300_Exists && event_20031_Exists
                    && ExecutorHelper.eventTimeFits(event_1300_Key, targetEventTime, eventStore)
                    && ExecutorHelper.eventTimeFits(event_20031_Key, targetEventTime, eventStore)) {

                String event_20031_Value = eventStore.get(event_20031_Key.toString());
                JsonObject event_20031_ValueJson = ExecutorHelper.getJsonObject(event_20031_Value);
                long event_20031_server_time = event_20031_ValueJson.get("server_time").getAsLong();

                int push_period = ExecutorHelper.getPushPeriodFromEvent(eventValue, 1800_00);
                ExecutorHelper.updatePushPeriod(push_period, kv.key, eventValue, eventStore);

                if (ExecutorHelper.isPushTime(event_20031_server_time, push_period)) {
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    eventStore.delete(kv.key);
                    eventStore.delete(event_1300_Key.toString());
                    eventStore.delete(event_20031_Key.toString());
                    eventStore.delete(event_20032_Key.toString());
                } else {
                    eventStore.delete(kv.key);
                    eventStore.delete(event_1300_Key.toString());
                    eventStore.delete(event_20031_Key.toString());
                    eventStore.delete(event_20032_Key.toString());
                }
            } else if (event_1300_Exists && event_20032_Exists
                    && ExecutorHelper.eventTimeFits(event_1300_Key, targetEventTime, eventStore)
                    && ExecutorHelper.eventTimeFits(event_20032_Key, targetEventTime, eventStore)) {

                String event_20032_Value = eventStore.get(event_20032_Key.toString());
                JsonObject event_20032_ValueJson = ExecutorHelper.getJsonObject(event_20032_Value);
                long event_20032_server_time = event_20032_ValueJson.get("server_time").getAsLong();

                int push_period = ExecutorHelper.getPushPeriodFromEvent(eventValue, 1800_00);
                ExecutorHelper.updatePushPeriod(push_period, kv.key, eventValue, eventStore);

                if (ExecutorHelper.isPushTime(event_20032_server_time, push_period)) {
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    eventStore.delete(kv.key);
                    eventStore.delete(event_1300_Key.toString());
                    eventStore.delete(event_20031_Key.toString());
                    eventStore.delete(event_20032_Key.toString());
                } else {
                    eventStore.delete(kv.key);
                    eventStore.delete(event_1300_Key.toString());
                    eventStore.delete(event_20031_Key.toString());
                    eventStore.delete(event_20032_Key.toString());
                }
            }
        } else {
            eventStore.delete(kv.key);
            eventStore.delete(event_1300_Key.toString());
        }
    }
}
