package executors.executors.eventCashExecutors;

import executors.actions.EventCashExecutor;
import org.apache.kafka.streams.state.KeyValueStore;

public class EventCash_2007_Executor implements EventCashExecutor {

    @Override
    public void executeEventCash(Object key, Object value, KeyValueStore<String, String> eventStore) {
//        EventKey eventKey = Utils.getEventKey(key.toString());
//        JsonObject eventValue = Utils.getJsonObject(value.toString());
//        boolean isFromPush = eventValue.get("from").getAsString().equals("push");
//
//        String user_id = eventKey.getUser_id();
//
//        if (!user_id.startsWith("Z") && isFromPush) {
//            eventStore.put(key.toString(), value.toString());
//        }
    }
}
