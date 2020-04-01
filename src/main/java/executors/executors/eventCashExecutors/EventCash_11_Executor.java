package executors.executors.eventCashExecutors;

import executors.actions.EventCashExecutor;
import model.EventKey;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Utils;

public class EventCash_11_Executor implements EventCashExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventCash_11_Executor.class);

    @Override
    public void executeEventCash(Object key, Object value, KeyValueStore<String, String> eventStore) {

        EventKey eventKey = Utils.getEventKey(key.toString());
        String user_id = eventKey.getUser_id();
        boolean eventExist = eventStore.get(key.toString()) == null; // TODO проверить результат

        String custom_dimension_1 = "";
        try {
            custom_dimension_1 = Utils.getJsonObject(value.toString()).get("custom_dimension_1").getAsString();
        } catch (NullPointerException e) {
            LOGGER.debug("Can't get parameter custom_dimension_1 from event.");
        }

        if (user_id.startsWith("Z") && custom_dimension_1.equals("OnboardingCard1") && eventExist) {
            eventStore.put(key.toString(), value.toString());
        }
    }
}
