package executors.executors.eventCashExecutors;

import executors.actions.EventCashExecutor;
import org.apache.kafka.streams.state.KeyValueStore;

public class EventCash_2010_Executor implements EventCashExecutor {

    @Override
    public void executeEventCash(Object key, Object value, KeyValueStore<String, String> eventStore) {

    }
}
