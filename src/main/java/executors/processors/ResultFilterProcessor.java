package executors.processors;

import executors.action.Action;
import executors.action.ScenarioCaseMapping;
import model.EventKey;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Utils;

import java.time.Duration;

public class ResultFilterProcessor implements Processor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResultFilterProcessor.class);

    private ProcessorContext context;
    private String eventStoreName;
    private KeyValueStore<String, String> eventStore;

    public ResultFilterProcessor(String eventStoreName) {
        this.eventStoreName = eventStoreName;
    }

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        eventStore = (KeyValueStore) processorContext.getStateStore(eventStoreName);
        this.context.schedule(Duration.ofSeconds(15), PunctuationType.WALL_CLOCK_TIME, schedule -> {
            LOGGER.info("Doing filtering...");
            doFilter();
            eventStore.all().forEachRemaining(
                    kv -> LOGGER.info("ResultFilterProcessor, eventStore: " + kv.key + "; value: " + kv.value));
        });
    }

    @Override
    public void process(Object key, Object value) {

    }

    @Override
    public void close() {

    }

    private void doFilter() {

        KeyValueIterator<String, String> iterator = eventStore.all();

        while (iterator.hasNext()) {

            KeyValue<String, String> kv = iterator.next();
            EventKey eventKey = Utils.getEventKey(kv.key);
            int scenario_id = eventKey.getScenario_id();
            String scenarioCase = ScenarioCaseMapping.getScenarioCase(scenario_id);

            if (scenarioCase != null) {
                Action.valueOf(scenarioCase).executeScenario(kv, eventStore);
            }
        }
    }
}
