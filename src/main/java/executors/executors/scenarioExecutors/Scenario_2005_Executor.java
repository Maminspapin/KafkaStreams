package executors.executors.scenarioExecutors;

import client.RecordProducer;
import com.google.gson.JsonObject;
import executors.actions.ScenarioExecutor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Utils;

import static config.Topics.RESULTS;

public class Scenario_2005_Executor implements ScenarioExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Scenario_2005_Executor.class);

    @Override
    public void executeScenario(KeyValue<String, String> kv, KeyValueStore<String, String> store) {
        JsonObject eventValue = Utils.getJsonObject(kv.value);
        int push_period = getPushPeriod(eventValue, 600_00);
        updatePushPeriod(push_period, kv.key, eventValue, store); // TODO здесь и в подобных перевести в if
        long server_time = eventValue.get("server_time").getAsLong();

        if (isPushTime(server_time, push_period)) {
            RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
            store.delete(kv.key);
        }
    }

    private void updatePushPeriod(int push_period, String key, JsonObject value, KeyValueStore<String, String> store) {
        value.addProperty("push_period", push_period);
        store.put(key, value.toString());
    }

    private void updateScenarioId(String key, JsonObject value, int scenario_id,  KeyValueStore<String, String> store) {
        value.addProperty("scenario_id", scenario_id);
        store.put(key, value.toString());
    }

    private boolean isPushTime(long eventTime, int push_period) { // TODO проверить и сделать везде
        DateTime pushTime = Utils.getPushTime(eventTime, push_period);
        DateTime nowTime = new DateTime(DateTimeZone.UTC);

        LOGGER.debug("now time: " + nowTime.toString());
        LOGGER.debug("push time: " + pushTime.toString());

        return nowTime.compareTo(pushTime) > 0;
    }

    private int getPushPeriod(JsonObject value, int initialPushPeriod) { // TODO проверить и сделать везде
        return value.get("push_period").getAsInt() == 0 ? initialPushPeriod : value.get("push_period").getAsInt();
    }
}

