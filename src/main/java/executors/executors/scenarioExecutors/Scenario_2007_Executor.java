package executors.executors.scenarioExecutors;

import executors.actions.ScenarioExecutor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;

public class Scenario_2007_Executor implements ScenarioExecutor {

    @Override
    public void executeScenario(KeyValue<String, String> kv, KeyValueStore<String, String> eventStore) {

    }

//    @Override
//    public void executeScenario(KeyValue<String, String> kv, KeyValueStore<String, String> store) {
//
//        EventKey eventKey = Utils.getEventKey(kv.key);
//        JsonObject eventValue = Utils.getJsonObject(kv.value);
//
//        long server_time = eventValue.get("server_time").getAsLong();
//        DateTime targetEventTime = Utils.getEventTime(server_time);
//
//        EventKey event_1300_Key = new EventKey(eventKey.getUser_id(), 1300);
//        boolean event_1300_Exists = eventExists(event_1300_Key, store); // TODO проверить
//
//        if (event_1300_Exists) {
//
//            String event_1300_Value = store.get(event_1300_Key.toString());
//            JsonObject event_1300_ValueJson = Utils.getJsonObject(event_1300_Value);
//            long event_1300_server_time = event_1300_ValueJson.get("server_time").getAsLong();
//            int push_period = getPushPeriod(event_1300_ValueJson, 1800_00);
//            DateTime event_1300_PushTime = Utils.getPushTime(event_1300_server_time, push_period);
//
//            if (event_1300_PushTime.compareTo(targetEventTime) > 0) {
//
//            }
//
//
//
//
//        }
//
//
//    }
//
//    private void updatePushPeriod(int push_period, String key, JsonObject value, KeyValueStore<String, String> store) {
//        value.addProperty("push_period", push_period);
//        store.put(key, value.toString());
//    }
//
//    private boolean eventTimeFits(EventKey conditionalEventKey, DateTime targetEventTime, KeyValueStore<String, String> store) {
//        String conditionalEventValue = store.get(conditionalEventKey.toString());
//        JsonObject conditionalEventValueJson = Utils.getJsonObject(conditionalEventValue);
//        DateTime conditionalEventTime = Utils.getEventTime(conditionalEventValueJson.get("server_time").getAsLong());
//        DateTime endOfDate = getEndOfDay(targetEventTime);
//
//        return endOfDate.compareTo(conditionalEventTime) > 0;
//    }
//
//    private boolean eventExists(EventKey conditionalEventKey, KeyValueStore<String, String> store) {
//        String loginEventValue = store.get(conditionalEventKey.toString());
//        return loginEventValue != null; // TODO проверить
//    }
//
//    private boolean isPushTime(long eventTime, int push_period) { // TODO проверить и сделать везде
//        DateTime pushTime = Utils.getPushTime(eventTime, push_period);
//        DateTime nowTime = new DateTime(DateTimeZone.UTC);
//
//        LOGGER.debug("now time: " + nowTime.toString());
//        LOGGER.debug("push time: " + pushTime.toString());
//
//        return nowTime.compareTo(pushTime) > 0;
//    }
//
//    private boolean isCurrentDay(DateTime dateTime) {
//        DateTime endOfDay = getEndOfDay(dateTime);
//        DateTime nowTime = new DateTime(DateTimeZone.UTC);
//
//        return endOfDay.compareTo(nowTime) > 0;
//    }
//
//    private int getPushPeriod(JsonObject value, int initialPushPeriod) { // TODO проверить и сделать везде
//        return value.get("push_period").getAsInt() == 0 ? initialPushPeriod : value.get("push_period").getAsInt();
//    }
//
//    private DateTime getEndOfDay(DateTime day) {
//        return new LocalDateTime(day.toDate())
//                .withHourOfDay(23)
//                .withMinuteOfHour(59)
//                .withSecondOfMinute(59)
//                .toDateTime(DateTimeZone.UTC);
//    }
}
