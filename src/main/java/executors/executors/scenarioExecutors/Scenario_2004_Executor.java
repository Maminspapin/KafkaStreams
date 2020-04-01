package executors.executors.scenarioExecutors;

import client.RecordProducer;
import com.google.gson.JsonObject;
import executors.actions.ScenarioExecutor;
import model.EventKey;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Utils;

import static config.Topics.RESULTS;

public class Scenario_2004_Executor implements ScenarioExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Scenario_2004_Executor.class);

    @Override
    public void executeScenario(KeyValue<String, String> kv, KeyValueStore<String, String> store) {

        EventKey eventKey = Utils.getEventKey(kv.key);
        JsonObject eventValue = Utils.getJsonObject(kv.value);

        long server_time = eventValue.get("server_time").getAsLong();
        DateTime targetEventTime = Utils.getEventTime(server_time);

        EventKey event_1300_Key = new EventKey(eventKey.getUser_id(), 1300);
        boolean event_1300_Exists = eventExists(event_1300_Key, store); // TODO проверить

        EventKey event_20031_Key = new EventKey(eventKey.getUser_id(), 20031);
        boolean event_20031_Exists = eventExists(event_20031_Key, store);

        EventKey event_20032_Key = new EventKey(eventKey.getUser_id(), 20032);
        boolean event_20032_Exists = eventExists(event_20032_Key, store);

        if (isCurrentDay(targetEventTime)) {

            if (event_1300_Exists && event_20031_Exists
                    && eventTimeFits(event_1300_Key, targetEventTime, store)
                    && eventTimeFits(event_20031_Key, targetEventTime, store)) {

                String event_20031_Value = store.get(event_20031_Key.toString());
                JsonObject event_20031_ValueJson = Utils.getJsonObject(event_20031_Value);
                long event_20031_server_time = event_20031_ValueJson.get("server_time").getAsLong();

                int push_period = getPushPeriod(eventValue, 1800_00);
                updatePushPeriod(push_period, kv.key, eventValue, store);

                if (isPushTime(event_20031_server_time, push_period)) {
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    store.delete(kv.key);
                    store.delete(event_1300_Key.toString());
                    store.delete(event_20031_Key.toString());
                    store.delete(event_20032_Key.toString());
                } else {
                    store.delete(kv.key);
                    store.delete(event_1300_Key.toString());
                    store.delete(event_20031_Key.toString());
                    store.delete(event_20032_Key.toString());
                }
            }

            if (event_1300_Exists && event_20032_Exists
                    && eventTimeFits(event_1300_Key, targetEventTime, store)
                    && eventTimeFits(event_20032_Key, targetEventTime, store)) {

                String event_20032_Value = store.get(event_20032_Key.toString());
                JsonObject event_20032_ValueJson = Utils.getJsonObject(event_20032_Value);
                long event_20032_server_time = event_20032_ValueJson.get("server_time").getAsLong();

                int push_period = getPushPeriod(eventValue, 1800_00);
                updatePushPeriod(push_period, kv.key, eventValue, store);

                if (isPushTime(event_20032_server_time, push_period)) {
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    store.delete(kv.key);
                    store.delete(event_1300_Key.toString());
                    store.delete(event_20031_Key.toString());
                    store.delete(event_20032_Key.toString());
                } else {
                    store.delete(kv.key);
                    store.delete(event_1300_Key.toString());
                    store.delete(event_20031_Key.toString());
                    store.delete(event_20032_Key.toString());
                }
            }
        } else {
            store.delete(kv.key);
            store.delete(event_1300_Key.toString());
        }
    }

    private void updatePushPeriod(int push_period, String key, JsonObject value, KeyValueStore<String, String> store) {
        value.addProperty("push_period", push_period);
        store.put(key, value.toString());
    }

    private boolean eventTimeFits(EventKey conditionalEventKey, DateTime targetEventTime, KeyValueStore<String, String> store) {
        String conditionalEventValue = store.get(conditionalEventKey.toString());
        JsonObject conditionalEventValueJson = Utils.getJsonObject(conditionalEventValue);
        DateTime conditionalEventTime = Utils.getEventTime(conditionalEventValueJson.get("server_time").getAsLong());
        DateTime endOfDate = getEndOfDay(targetEventTime);

        return endOfDate.compareTo(conditionalEventTime) > 0;
    }

    private boolean eventExists(EventKey conditionalEventKey, KeyValueStore<String, String> store) {
        String loginEventValue = store.get(conditionalEventKey.toString());
        return loginEventValue != null; // TODO проверить
    }

    private boolean isPushTime(long eventTime, int push_period) { // TODO проверить и сделать везде
        DateTime pushTime = Utils.getPushTime(eventTime, push_period);
        DateTime nowTime = new DateTime(DateTimeZone.UTC);

        LOGGER.debug("now time: " + nowTime.toString());
        LOGGER.debug("push time: " + pushTime.toString());

        return nowTime.compareTo(pushTime) > 0;
    }

    private boolean isCurrentDay(DateTime dateTime) {
        DateTime endOfDay = getEndOfDay(dateTime);
        DateTime nowTime = new DateTime(DateTimeZone.UTC);

        return endOfDay.compareTo(nowTime) > 0;
    }

    private int getPushPeriod(JsonObject value, int initialPushPeriod) { // TODO проверить и сделать везде
        return value.get("push_period").getAsInt() == 0 ? initialPushPeriod : value.get("push_period").getAsInt();
    }

    private DateTime getEndOfDay(DateTime day) {
        return new LocalDateTime(day.toDate())
                .withHourOfDay(23)
                .withMinuteOfHour(59)
                .withSecondOfMinute(59)
                .toDateTime(DateTimeZone.UTC);
    }
}
