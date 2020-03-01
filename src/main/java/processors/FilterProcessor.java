package processors;

import client.ResultsProducer;
import com.google.gson.Gson;
import model.EventKey;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import static config.Topics.RESULTS;

public class FilterProcessor implements Processor {

    private ProcessorContext context;
    private String eventStoreName;
    private KeyValueStore<String, String> eventStore;

    public FilterProcessor(String eventStoreName) {
        this.eventStoreName = eventStoreName;
    }

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        eventStore = (KeyValueStore) processorContext.getStateStore(eventStoreName);
        this.context.schedule(Duration.ofSeconds(15), PunctuationType.WALL_CLOCK_TIME, schedule -> {
            System.out.println("Doing filtering...");
            doFilter();
        });

    }

    @Override
    public void process(Object key, Object value) {
        System.out.println("FilterProcessor is processing...");
    }

    @Override
    public void close() {

    }

    private void doFilter() {

        KeyValueIterator<String, String> iterator = eventStore.all();

        while(iterator.hasNext()) {

            KeyValue<String, String> kv = iterator.next();
            EventKey eventKey = new Gson().fromJson(kv.key, EventKey.class);
            Map<String, Object> eventValue = new Gson().fromJson(kv.value, Map.class);

            Date after = getAfter(eventValue.get("server_time").toString(), 600_000);
            Date now = new Date();

            if (eventKey.getEvent_id() == 1 && now.compareTo(after) > 0) {
                ResultsProducer.sendRecord(RESULTS.topicName(), kv.key, kv.value);
                eventStore.delete(kv.key); // TODO удалять events старше определенного времени
            }
        }

        eventStore.all().forEachRemaining(
                kkvv -> System.out.println("FilterProcessor, eventStore: " + kkvv.key + "; value: " + kkvv.value));
    }

    private Date getAfter (String server_time, long period) {

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        Date dateEvent = null;
        try {
            dateEvent = formatter.parse(server_time);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return new Date(dateEvent.getTime() + period);
    }

}
