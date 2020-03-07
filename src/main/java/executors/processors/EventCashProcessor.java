package executors.processors;

import model.EventKey;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import utils.Utils;

public class EventCashProcessor implements Processor {

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

        if (eventKey.getScenario_id() == 1) {
            eventStore.put(key.toString(), value.toString());
        }

        if (eventKey.getScenario_id() == 3) {
            KeyValueIterator<String, String> iterator = eventStore.all();
            while (iterator.hasNext()) {
                KeyValue<String, String> kv = iterator.next();
                EventKey innerEventKey = Utils.getEventKey(kv.key);

                if (innerEventKey.getUser_id() == eventKey.getUser_id() && innerEventKey.getScenario_id() == 1) {
                    eventStore.delete(kv.key);
                }
            }
        }
    }

    @Override
    public void close() {

    }
}