package executors.executors.eventCashExecutors;

import executors.actions.EventCashExecutor;
import model.EventKey;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Utils;

public class EventCash_20031_Executor implements EventCashExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventCash_20031_Executor.class);

    @Override
    public void executeEventCash(Object key, Object value, KeyValueStore<String, String> eventStore) {

        EventKey eventKey = Utils.getEventKey(key.toString());
        String user_id = eventKey.getUser_id();

        LOGGER.info("Cancel event for [scenario 2003]. Key: " + key.toString() + ", Value: " + value.toString());

        EventKey scenario_2003_key = new EventKey(user_id, 2003);
        eventStore.delete(scenario_2003_key.toString());

        EventKey scenario_1300_key = new EventKey(user_id, 1300);
        eventStore.delete(scenario_1300_key.toString());

        eventStore.put(key.toString(), value.toString()); // 2004
    }
}
