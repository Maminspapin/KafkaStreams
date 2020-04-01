package executors.executors.eventCashExecutors;

import executors.actions.EventCashExecutor;
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

        LOGGER.info("Cancel event for [scenario 21]. Key: " + key.toString() + ", Value: " + value.toString());

        EventKey scenario_21_key = new EventKey(user_id, 21);
        eventStore.delete(scenario_21_key.toString());

        EventKey scenario_2002_key = new EventKey(user_id, 2002);
        eventStore.delete(scenario_2002_key.toString());

        eventStore.put(key.toString(), value.toString()); // 2003, 2004

        EventKey scenario_2005_key = new EventKey(user_id, 2005);
        eventStore.delete(scenario_2005_key.toString());
    }
}
