package executors.action;

import org.apache.kafka.streams.state.KeyValueStore;

public interface EventCashExecutor {

    void executeEventCash(Object key, Object value, KeyValueStore<String, String> eventStore);
}
