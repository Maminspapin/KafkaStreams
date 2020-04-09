package executors.executors.scenarioExecutors;

import client.RecordProducer;
import com.google.gson.JsonObject;
import executors.actions.ScenarioExecutor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import executors.helper.ExecutorHelper;

import static config.Topics.RESULTS;

public class Scenario_21_Executor implements ScenarioExecutor {

    @Override
    public void executeScenario(KeyValue<String, String> kv, KeyValueStore<String, String> eventStore) {

        JsonObject eventValue = ExecutorHelper.getJsonObject(kv.value);
        int push_period = ExecutorHelper.getPushPeriodFromEvent(eventValue, 600_00);
        long server_time = eventValue.get("server_time").getAsLong();

        if (ExecutorHelper.isPushTime(server_time, push_period)) {

            ExecutorHelper.updatePushPeriod(push_period, kv.key, eventValue, eventStore);

            switch (push_period) {
                case (600_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(1_200_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(23, kv.key, eventValue, eventStore);
                    break;
                case (1_200_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(1_800_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(25, kv.key, eventValue, eventStore);
                    break;
                case (1_800_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(2_400_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(27, kv.key, eventValue, eventStore);
                    break;
                case (2_400_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(3_000_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(29, kv.key, eventValue, eventStore);
                    break;
                case (3_000_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(3_600_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(211, kv.key, eventValue, eventStore);
                    break;
                case (3_600_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(4_200_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(213, kv.key, eventValue, eventStore);
                    break;
                case (4_200_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(4_800_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(215, kv.key, eventValue, eventStore);
                    break;
                case (4_800_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(5_400_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(217, kv.key, eventValue, eventStore);
                    break;
                case (5_400_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(6_000_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(219, kv.key, eventValue, eventStore);
                    break;
                case (6_000_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(6_600_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(221, kv.key, eventValue, eventStore);
                    break;
                case (6_600_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(7_200_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(223, kv.key, eventValue, eventStore);
                    break;
                case (7_200_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(7_800_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(225, kv.key, eventValue, eventStore);
                    break;
                case (7_800_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(8_400_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(227, kv.key, eventValue, eventStore);
                    break;
                case (8_400_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(9_000_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(229, kv.key, eventValue, eventStore);
                    break;
                case (9_000_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(9_600_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(231, kv.key, eventValue, eventStore);
                    break;
                default:
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    eventStore.delete(kv.key);
            }
        }
    }
}
