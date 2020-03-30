package executors.processors;

import executors.action.Action;
import executors.action.ScenarioCaseMapping;
import model.EventKey;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
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

        int scenario_id = Utils.getEventKey(key.toString()).getScenario_id();
        String scenarioCase = ScenarioCaseMapping.getScenarioCase(scenario_id);

        if (scenarioCase != null) {
            Action.valueOf(scenarioCase).executeEventCash(key, value, eventStore);
        }
    }

    @Override
    public void close() {

    }
}
