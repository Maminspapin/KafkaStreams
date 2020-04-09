package executors.executors.eventCashExecutors;

import executors.actions.EventCashExecutor;
import model.EventKey;
import org.apache.kafka.streams.state.KeyValueStore;
import executors.helper.ExecutorHelper;

public class EventCash_2003_Executor implements EventCashExecutor {

    @Override
    public void executeEventCash(Object key, Object value, KeyValueStore<String, String> eventStore) {

        EventKey eventKey = ExecutorHelper.getEventKey(key.toString());
        String user_id = eventKey.getUser_id();

        if (user_id.startsWith("Z")) {
            eventStore.put(key.toString(), value.toString());
        }
    }
}
