package executors.executors.eventCashExecutors;

import executors.actions.EventCashExecutor;
import model.EventKey;
import org.apache.kafka.streams.state.KeyValueStore;
import utils.Utils;

public class EventCash_2005_Executor implements EventCashExecutor {

    @Override
    public void executeEventCash(Object key, Object value, KeyValueStore<String, String> eventStore) {
        EventKey eventKey = Utils.getEventKey(key.toString());
        String user_id = eventKey.getUser_id();

        if (!user_id.startsWith("Z")) {
            eventStore.put(key.toString(), value.toString());
        }
    }
}
