package executors.processors;

import model.EventKey;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Utils;

public class EventCashProcessor implements Processor {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventCashProcessor.class);

    private ProcessorContext context;
    private String eventStoreName;
    private KeyValueStore<String, String> eventStore;

    public EventCashProcessor(String eventStoreName) {
        this.eventStoreName = eventStoreName;
    }

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        eventStore = (KeyValueStore) processorContext.getStateStore(eventStoreName);
    }

    @Override
    public void process(Object key, Object value) {

        EventKey eventKey = Utils.getEventKey(key.toString());
        int scenario_id = eventKey.getScenario_id();
        String user_id = eventKey.getUser_id();
        String custom_dimesion_1 = "";
        boolean eventExist = eventStore.get(key.toString()) == null;

        try {
            custom_dimesion_1 = Utils.getJsonObject(value.toString()).get("custom_dimension_1").getAsString();
        } catch (NullPointerException e) {
            LOGGER.debug("Can't get parameter custom_dimension_1 from event.");
        }

        if (scenario_id == 11 // TODO цепочку if упростить
                && user_id.startsWith("Z")
                && custom_dimesion_1.equals("OnboardingCard1")
                && eventExist) {

            eventStore.put(key.toString(), value.toString());
        }

        if (scenario_id == 1100 || scenario_id == 1300) {

            LOGGER.info("Cancel event for [scenario 1]. Key: " + key.toString() + ", Value: " + value.toString());

            EventKey scenario_1_key = new EventKey(user_id, 11);
            eventStore.delete(scenario_1_key.toString());

            if (scenario_id == 1100 && user_id.startsWith("Z")) {
                eventStore.put(key.toString(), value.toString());
            }
        }
    }

    @Override
    public void close() {

    }
}
