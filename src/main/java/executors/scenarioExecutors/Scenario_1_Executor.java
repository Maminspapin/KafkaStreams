package executors.scenarioExecutors;

import client.RecordProducer;
import com.google.gson.JsonObject;
import executors.ScenarioExecutor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import utils.Utils;

import java.util.Date;

import static config.Topics.EVENTS;
import static config.Topics.RESULTS;

public class Scenario_1_Executor implements ScenarioExecutor {

    @Override
    public void execute(KeyValue<String, String> kv, KeyValueStore<String, String> store) {

        JsonObject eventValue = Utils.getJsonObject(kv.value);
        int push_period = eventValue.get("push_period").getAsInt() == 0 ? 600000 : Integer.valueOf(eventValue.get("push_period").toString());

        Date after = Utils.getAfter(eventValue.get("server_time").getAsString(), push_period);
        Date now = new Date();

        if (now.compareTo(after) > 0) {

            switch (push_period) {
                case(600_000):
                    push_period = 1_200_000;
                    eventValue.addProperty("push_period", push_period);
                    RecordProducer.sendRecord(EVENTS.topicName(), kv.key, eventValue.toString());
                    break;
                case(1_200_000):
                    push_period = 1_800_000;
                    eventValue.addProperty("push_period", push_period);
                    RecordProducer.sendRecord(EVENTS.topicName(), kv.key, eventValue.toString());
                    break;
                default:
                    store.delete(kv.key);
            }

            RecordProducer.sendRecord(RESULTS.topicName(), kv.key, kv.value);
        }
    }
}
