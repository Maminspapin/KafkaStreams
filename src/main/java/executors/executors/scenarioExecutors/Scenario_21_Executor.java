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

public class Scenario_21_Executor implements ScenarioExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Scenario_21_Executor.class);

    @Override
    public void executeScenario(KeyValue<String, String> kv, KeyValueStore<String, String> store) {

        JsonObject eventValue = Utils.getJsonObject(kv.value);
        int push_period = eventValue.get("push_period").getAsInt() == 0 ? 600_00 : eventValue.get("push_period").getAsInt();
        updatePushPeriod(push_period, kv.key, eventValue, store);

        DateTime pushTime = Utils.getPushTime(eventValue.get("server_time").getAsString(), push_period);
        DateTime nowTime = new DateTime(DateTimeZone.UTC);

        LOGGER.debug("now time: " + nowTime.toString());
        LOGGER.debug("push time: " + pushTime.toString());

        if (nowTime.compareTo(pushTime) > 0) {

            switch (push_period) {
                case (600_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    updatePushPeriod(1_200_00, kv.key, eventValue, store);
                    updateScenarioId(kv.key, eventValue, 23, store);
                    break;
                case (1_200_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    updatePushPeriod(1_800_00, kv.key, eventValue, store);
                    updateScenarioId(kv.key, eventValue, 25, store);
                    break;
                case (1_800_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    updatePushPeriod(2_400_00, kv.key, eventValue, store);
                    updateScenarioId(kv.key, eventValue, 27, store);
                    break;
                case (2_400_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    updatePushPeriod(3_000_00, kv.key, eventValue, store);
                    updateScenarioId(kv.key, eventValue, 29, store);
                    break;
                case (3_000_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    updatePushPeriod(3_600_00, kv.key, eventValue, store);
                    updateScenarioId(kv.key, eventValue, 211, store);
                    break;
                case (3_600_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    updatePushPeriod(4_200_00, kv.key, eventValue, store);
                    updateScenarioId(kv.key, eventValue, 213, store);
                    break;
                case (4_200_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    updatePushPeriod(4_800_00, kv.key, eventValue, store);
                    updateScenarioId(kv.key, eventValue, 215, store);
                    break;
                case (4_800_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    updatePushPeriod(5_400_00, kv.key, eventValue, store);
                    updateScenarioId(kv.key, eventValue, 217, store);
                    break;
                case (5_400_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    updatePushPeriod(6_000_00, kv.key, eventValue, store);
                    updateScenarioId(kv.key, eventValue, 219, store);
                    break;
                case (6_000_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    updatePushPeriod(6_600_00, kv.key, eventValue, store);
                    updateScenarioId(kv.key, eventValue, 221, store);
                    break;
                case (6_600_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    updatePushPeriod(7_200_00, kv.key, eventValue, store);
                    updateScenarioId(kv.key, eventValue, 223, store);
                    break;
                case (7_200_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    updatePushPeriod(7_800_00, kv.key, eventValue, store);
                    updateScenarioId(kv.key, eventValue, 225, store);
                    break;
                case (7_800_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    updatePushPeriod(8_400_00, kv.key, eventValue, store);
                    updateScenarioId(kv.key, eventValue, 227, store);
                    break;
                case (8_400_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    updatePushPeriod(9_000_00, kv.key, eventValue, store);
                    updateScenarioId(kv.key, eventValue, 229, store);
                    break;
                case (9_000_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    updatePushPeriod(9_600_00, kv.key, eventValue, store);
                    updateScenarioId(kv.key, eventValue, 231, store);
                    break;
                default:
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    store.delete(kv.key);
            }
        }
    }

    private void updatePushPeriod(int push_period, String key, JsonObject value, KeyValueStore<String, String> store) {
        value.addProperty("push_period", push_period);
        store.put(key, value.toString());
    }

    private void updateScenarioId(String key, JsonObject value, int scenario_id, KeyValueStore<String, String> store) {
        value.addProperty("scenario_id", scenario_id);
        store.put(key, value.toString());
    }
}
