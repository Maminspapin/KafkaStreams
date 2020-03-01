package transformers;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import model.EventKey;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

public class EventKeyTransformer implements Transformer {

    @Override
    public void init(ProcessorContext processorContext) {

    }

    @Override
    public Object transform(Object key, Object value) {

        EventKey eventKey = new EventKey();
        int user_id = 0;
        int scenario_id = 0;

        try {
            user_id = new Gson().fromJson(value.toString(), JsonObject.class).get("user_id").getAsInt();
        } catch (NullPointerException e) {
            System.out.println("No user detected...");
        }

        try {
            scenario_id = new Gson().fromJson(value.toString(), JsonObject.class).get("scenario_id").getAsInt();
        } catch (NullPointerException e) {
            System.out.println("No scenario detected...");
        }


        eventKey.setUser_id(user_id);
        eventKey.setEvent_id(scenario_id);

        return new KeyValue<>(eventKey.toString(), value.toString());
    }

    @Override
    public void close() {

    }
}
