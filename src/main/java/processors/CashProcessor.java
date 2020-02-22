package processors;

import com.google.gson.Gson;
import model.EventKey;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class CashProcessor implements Processor {

    private ProcessorContext context;
    private String eventStoreName;
    private KeyValueStore<String, String> eventStore;
    int i = 0;

    public CashProcessor() {

    }

    public CashProcessor(String eventStoreName) {
        this.eventStoreName = eventStoreName;
    }

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        eventStore = (KeyValueStore) processorContext.getStateStore(eventStoreName);
    }

    @Override
    public void process(Object key, Object value) {

        if (i == 0) {
            KeyValueIterator<String, String> iterator = eventStore.all();
            while (iterator.hasNext()) {
                KeyValue<String, String> kv = iterator.next();
                System.out.println("Before! " + kv.key + "; " + kv.value);
            }
        }

        i++;
        eventStore.put(key.toString(), value.toString()); // складываем в store

        EventKey eventKey = new Gson().fromJson(key.toString(), EventKey.class);
        if (eventKey.getEvent_id() == 3) { // если событие - отменяющее, удаляем из store событие по пользователю для отправки push
            KeyValueIterator<String, String> iterator = eventStore.all();
            while (iterator.hasNext()) {
                KeyValue<String, String> kv = iterator.next();
                EventKey innerEventKey = new Gson().fromJson(kv.key, EventKey.class);

                if (innerEventKey.getUser_id() == eventKey.getUser_id() && innerEventKey.getEvent_id() == 0) {
                    eventStore.delete(kv.key);
                }
            }
        }

        eventStore.all().forEachRemaining(
                kv -> System.out.println(i + " After! key: " + kv.key + "; value: " + kv.value)
        );
    }

    @Override
    public void close() {

    }
}
