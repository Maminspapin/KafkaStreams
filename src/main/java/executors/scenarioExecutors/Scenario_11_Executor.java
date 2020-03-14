package executors.scenarioExecutors;

import client.RecordProducer;
import com.google.gson.JsonObject;
import executors.ScenarioExecutor;
import executors.processors.ResultFilterProcessor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static config.Topics.RESULTS;

public class Scenario_11_Executor implements ScenarioExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResultFilterProcessor.class);

    @Override
    public void execute(KeyValue<String, String> kv, KeyValueStore<String, String> store) {

        JsonObject eventValue = Utils.getJsonObject(kv.value);
        int push_period = eventValue.get("push_period").getAsInt() == 0 ? 600_000 : Integer.valueOf(eventValue.get("push_period").toString());
        updatePushPeriod(push_period, kv.key, eventValue, store);

        DateTime after = Utils.getAfter(eventValue.get("server_time").getAsString(), push_period);
        DateTime now = new DateTime(DateTimeZone.UTC);

        if (after == null) {
            store.delete(kv.key);
            return;
        }

        if (now.compareTo(after) > 0) {
            
            LOGGER.info("now time: " + now.toString());
            LOGGER.info("after time: " + after.toString());

            switch (push_period) {
                case(600_000):
                    //eventValue.addProperty("text", getScenarioText("11"));
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    updatePushPeriod(1_200_000, kv.key, eventValue, store);
                    break;
                case(1_200_000):
                    //eventValue.addProperty("text", getScenarioText("12"));
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    updatePushPeriod(1_800_000, kv.key, eventValue, store);
                    break;
                default:
                    //eventValue.addProperty("text", getScenarioText("13"));
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    store.delete(kv.key);
            }
        }
    }

    private void updatePushPeriod(int push_period, String key, JsonObject value, KeyValueStore<String, String> store) {
        value.addProperty("push_period", push_period);
        store.put(key, value.toString());
    }

    private String getScenarioText(String subScenario) {
        try(InputStream input = new FileInputStream("C:\\Users\\LyulchenkoYN\\Texts\\texts.properties")) {

            Properties properties = new Properties();
            properties.load(input);

            return properties.getProperty(subScenario);
        } catch (IOException e) {
            e.printStackTrace();
            return "no text";
        }
    }
}