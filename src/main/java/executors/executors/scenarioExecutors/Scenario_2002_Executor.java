package executors.executors.scenarioExecutors;

import client.RecordProducer;
import com.google.gson.JsonObject;
import executors.actions.ScenarioExecutor;
import executors.executors.eventCashExecutors.EventCash_21_Executor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Utils;

import static config.Topics.RESULTS;

public class Scenario_2002_Executor implements ScenarioExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Scenario_2002_Executor.class);

    @Override
    public void executeScenario(KeyValue<String, String> kv, KeyValueStore<String, String> store) {
        JsonObject eventValue = Utils.getJsonObject(kv.value);
        int push_period = eventValue.get("push_period").getAsInt() == 0 ? 600_00 : eventValue.get("push_period").getAsInt();
        updatePushPeriod(push_period, kv.key, eventValue, store);

        DateTime pushTime = Utils.getPushTime(eventValue.get("server_time").getAsLong(), push_period);
        DateTime nowTime = new DateTime(DateTimeZone.UTC);

        LOGGER.debug("now time: " + nowTime.toString());
        LOGGER.debug("push time: " + pushTime.toString());

        if (nowTime.compareTo(pushTime) > 0) {
            RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
            store.delete(kv.key);
        }
    }

    private void updatePushPeriod(int push_period, String key, JsonObject
            value, KeyValueStore<String, String> store) {
        value.addProperty("push_period", push_period);
        store.put(key, value.toString());
    }
}

