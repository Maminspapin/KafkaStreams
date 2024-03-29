package executors.transformers;

import com.google.gson.JsonObject;
import model.EventKey;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import executors.helper.ExecutorHelper;

public class EventKeyTransformer implements Transformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventKeyTransformer.class);

    @Override
    public void init(ProcessorContext processorContext) {

    }

    @Override
    public Object transform(Object key, Object value) {

        JsonObject visitValueJson = ExecutorHelper.getJsonObject(value.toString());
        EventKey eventKey = new EventKey();
        String user_id;
        int scenario_id = 0;

        try {
            user_id = visitValueJson.get("user_id").getAsString();
        } catch (NullPointerException e) {
            user_id = "unknown_user";
            LOGGER.debug("No user detected...");
        }

        try {
            scenario_id = visitValueJson.get("scenario_id").getAsInt();
        } catch (NullPointerException e) {
            LOGGER.debug("No scenario detected...");
        }

        eventKey.setUser_id(user_id);
        eventKey.setScenario_id(scenario_id);

        return new KeyValue<>(eventKey.toString(), value.toString());
    }

    @Override
    public void close() {

    }
}
