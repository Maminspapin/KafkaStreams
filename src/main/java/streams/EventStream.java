package streams;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import model.CommonKey;
import model.EventKey;
import model.ScenariosDirectory;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import utils.Config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static utils.Topics.*;

public class EventStream {

    public static KafkaStreams newStream() {

        // TODO дубликаты в топике events! Не допускать дублирования
        try {
            Properties properties = Config.getProperties("EventStream");

            List<ScenariosDirectory> list = new ArrayList<>();

            StreamsBuilder builder = new StreamsBuilder();

            KStream<GenericRecord, GenericRecord> resourceScenarioStream = builder.stream(MATOMO_SCENARIOS_DIRECTORY.topicName()); // TODO переделать на KTable для правильного join
            KStream<GenericRecord, String> resultScenarioStream = resourceScenarioStream.mapValues(record -> record.get("after").toString());

            resultScenarioStream.foreach((key, value) -> list.add(new Gson().fromJson(value, ScenariosDirectory.class)));

            ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
            executorService.scheduleAtFixedRate(() -> System.out.println("List: " + list.size()), 0, 1, TimeUnit.MINUTES);

            KStream<GenericRecord, GenericRecord> resourceLinkVisitActionStream = builder.stream(MATOMO_LOG_LINK_VISIT_ACTION.topicName());
            KStream<GenericRecord, String> resultLinkVisitActionStream = resourceLinkVisitActionStream.mapValues(record -> record.get("after").toString());
            KStream<GenericRecord, String> filterLinkVisitActionStream = resultLinkVisitActionStream.filter((key, value) -> {

                for (int i = 0; i < list.size(); i++) {

                    Map linkVisitActionMap = new Gson().fromJson(value, Map.class);
                    int categoryId = list.get(i).getCategory_id();
                    int actionId = list.get(i).getAction_id();

                    if (null != linkVisitActionMap.get("category_id")
                            && null != linkVisitActionMap.get("action_id")
                            && linkVisitActionMap.get("category_id").equals(Double.valueOf(categoryId))
                            && linkVisitActionMap.get("action_id").equals(Double.valueOf(actionId))) {
                        return true;
                    }
                }

                return false;
            });

            KStream<String, String> toJoinLinkVisitActionStream = filterLinkVisitActionStream.selectKey((key, value) -> {

                CommonKey commonKey = new CommonKey();
                commonKey.setCategory_id(new Gson().fromJson(value, JsonObject.class).get("category_id").getAsInt());
                commonKey.setAction_id(new Gson().fromJson(value, JsonObject.class).get("action_id").getAsInt());

                return commonKey.toString();
            });

            KStream<String, String> toJoinScenarioStream = resultScenarioStream.selectKey((key, value) -> {

                CommonKey commonKey = new CommonKey();
                commonKey.setCategory_id(new Gson().fromJson(value, JsonObject.class).get("category_id").getAsInt());
                commonKey.setAction_id(new Gson().fromJson(value, JsonObject.class).get("action_id").getAsInt());

                return commonKey.toString();
            });

            Serde<String> stringSerde = Serdes.String();

            KStream<String, String> joinedStream = toJoinLinkVisitActionStream.join(toJoinScenarioStream, (visit, scenario) -> {

                String result = "";

                if (scenario != null) {
                    String scenario_id = new Gson().fromJson(scenario, JsonObject.class).get("scenario_id").toString();
                    String description = new Gson().fromJson(scenario, JsonObject.class).get("description").toString();
                    result = visit.replace("}", ", \"scenario_id\":" + scenario_id + ", \"description\":" + description + "}");
                    return result;
                } else {
                    return result;
                }
            }, JoinWindows.of(Duration.ofDays(365)), Joined.with(stringSerde, stringSerde, stringSerde));

            KStream<String, String> toEventStream = joinedStream.selectKey((key, value) -> { // TODO переделать на EventKey, написать Serde для дальнейшей удобной обработки

                EventKey eventKey = new EventKey();

                try {
                    eventKey.setUser_id(new Gson().fromJson(value, JsonObject.class).get("user_id").getAsInt());
                    eventKey.setEvent_id(new Gson().fromJson(value, JsonObject.class).get("scenario_id").getAsInt());
                } catch (Exception e) {
                    return new EventKey().toString();
                }

                return eventKey.toString();

            });

            toEventStream.to("events", Produced.with(stringSerde, stringSerde));

            Topology topology = builder.build();

            return new KafkaStreams(topology, properties);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return null;
        }
    }
}
