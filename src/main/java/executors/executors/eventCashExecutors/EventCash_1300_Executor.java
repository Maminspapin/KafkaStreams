package executors.executors.eventCashExecutors;

import executors.action.EventCashExecutor;
import model.EventKey;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Utils;

public class EventCash_1300_Executor implements EventCashExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventCash_1300_Executor.class);

    @Override
    public void executeEventCash(Object key, Object value, KeyValueStore<String, String> eventStore) {

        EventKey eventKey = Utils.getEventKey(key.toString());
        String user_id = eventKey.getUser_id();

        LOGGER.info("Cancel event for [scenario 1]. Key: " + key.toString() + ", Value: " + value.toString());

        EventKey scenario_1_key = new EventKey(user_id, 11);
        eventStore.delete(scenario_1_key.toString());
    }
}
