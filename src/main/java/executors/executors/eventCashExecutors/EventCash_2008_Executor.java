package executors.executors.eventCashExecutors;

import com.google.gson.JsonObject;
import executors.actions.EventCashExecutor;
import model.EventKey;
import org.apache.kafka.streams.state.KeyValueStore;
import org.joda.time.DateTime;
import executors.helper.ExecutorHelper;

public class EventCash_2008_Executor implements EventCashExecutor {

    @Override
    public void executeEventCash(Object key, Object value, KeyValueStore<String, String> eventStore) {

        EventKey targetEventKey = ExecutorHelper.getEventKey(key.toString());
        EventKey event_1300_Key = new EventKey(targetEventKey.getUser_id(), 1300);
        boolean event_1300_Exists = ExecutorHelper.eventExists(event_1300_Key, eventStore); // TODO проверить и это другое событие должно быть

        if (event_1300_Exists) {

            String user_id = targetEventKey.getUser_id();

            JsonObject targetEventValue = ExecutorHelper.getJsonObject(value.toString());
            long server_time = targetEventValue.get("server_time").getAsLong();
            DateTime targetEventTime = ExecutorHelper.getEventTime(server_time);

            String event_1300_Value = eventStore.get(event_1300_Key.toString());
            JsonObject event_1300_ValueJson = ExecutorHelper.getJsonObject(event_1300_Value);
            long event_1300_server_time = event_1300_ValueJson.get("server_time").getAsLong();
            int push_period = ExecutorHelper.getPushPeriodFromEvent(targetEventValue, 1800_00);
            DateTime event_1300_PushTime = ExecutorHelper.getPushTime(event_1300_server_time, push_period);

            if (!user_id.startsWith("Z") && targetEventTime.compareTo(event_1300_PushTime) <= 0) {
                eventStore.put(key.toString(), value.toString());
            }
        }
    }
}
