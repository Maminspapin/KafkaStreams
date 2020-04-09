package executors.executors.eventCashExecutors;

import executors.actions.EventCashExecutor;
import model.EventKey;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import executors.helper.ExecutorHelper;

public class EventCash_1500_Executor implements EventCashExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventCash_1500_Executor.class);

    @Override
    public void executeEventCash(Object key, Object value, KeyValueStore<String, String> eventStore) {

        EventKey eventKey = ExecutorHelper.getEventKey(key.toString());
        String user_id = eventKey.getUser_id();

        LOGGER.info("Cancel event for [scenario 21]. Key: " + key.toString() + ", Value: " + value.toString());

        EventKey scenario_1100_key = new EventKey(user_id, 21);
        eventStore.delete(scenario_1100_key.toString());
    }
}
