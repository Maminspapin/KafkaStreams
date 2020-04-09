package executors.processors;

import executors.actions.Action;
import executors.actions.ScenarioCaseMapping;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import executors.helper.ExecutorHelper;

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

        int scenario_id = ExecutorHelper.getEventKey(key.toString()).getScenario_id();
        String scenarioCase = ScenarioCaseMapping.getScenarioCase(scenario_id);

        if (scenarioCase != null) {
            Action.valueOf(scenarioCase).executeEventCash(key, value, eventStore);
        }
    }

    @Override
    public void close() {

    }
}
