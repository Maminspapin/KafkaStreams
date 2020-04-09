package executors.executors.scenarioExecutors;

import client.RecordProducer;
import com.google.gson.JsonObject;
import executors.actions.ScenarioExecutor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import executors.helper.ExecutorHelper;

import static config.Topics.RESULTS;

public class Scenario_11_Executor implements ScenarioExecutor {

    @Override
    public void executeScenario(KeyValue<String, String> kv, KeyValueStore<String, String> eventStore) {

        JsonObject eventValue = ExecutorHelper.getJsonObject(kv.value);
        int push_period = ExecutorHelper.getPushPeriodFromEvent(eventValue, 600_00);
        long server_time = eventValue.get("server_time").getAsLong();

        if (ExecutorHelper.isPushTime(server_time, push_period)) {

            ExecutorHelper.updatePushPeriod(push_period, kv.key, eventValue, eventStore);

            switch (push_period) {
                case(600_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(1_200_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(13, kv.key, eventValue, eventStore);
                    break;
                case(1_200_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(1_800_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(15, kv.key, eventValue, eventStore);
                    break;
                case(1_800_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(2_400_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(17, kv.key, eventValue, eventStore);
                    break;
                case(2_400_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(3_000_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(19, kv.key, eventValue, eventStore);
                    break;
                case(3_000_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(3_600_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(111, kv.key, eventValue, eventStore);
                    break;
                case(3_600_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(4_200_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(113, kv.key, eventValue, eventStore);
                    break;
                case(4_200_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(4_800_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(115, kv.key, eventValue, eventStore);
                    break;
                case(4_800_00):
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    ExecutorHelper.updatePushPeriod(5_400_00, kv.key, eventValue, eventStore);
                    ExecutorHelper.updateScenarioId(117, kv.key, eventValue, eventStore);
                    break;
                default:
                    RecordProducer.sendRecord(RESULTS.topicName(), kv.key, eventValue.toString());
                    eventStore.delete(kv.key);
            }
        }
    }
}
