package executors.processors;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class ScenarioCashProcessor implements Processor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScenarioCashProcessor.class);

    private ProcessorContext context;
    private String scenarioStoreName;
    private KeyValueStore<String, String> scenarioStore;

    public ScenarioCashProcessor(String scenarioStoreName) {
        this.scenarioStoreName = scenarioStoreName;
    }

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        scenarioStore = (KeyValueStore) processorContext.getStateStore(scenarioStoreName);

        this.context.schedule(Duration.ofMinutes(5), PunctuationType.WALL_CLOCK_TIME, schedule -> scenarioStore.all().forEachRemaining(kv -> LOGGER.info("Scenario. Key: " + kv.key + ", Value: " + kv.value)));
    }

    @Override
    public void process(Object key, Object value) {
        scenarioStore.put(key.toString(), value.toString());
    }

    @Override
    public void close() {

    }
}
