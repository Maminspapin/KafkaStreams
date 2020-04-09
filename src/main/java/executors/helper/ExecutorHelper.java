package executors.helper;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import model.EventKey;
import org.apache.kafka.streams.state.KeyValueStore;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorHelper.class);

    public static DateTime getPushTime(long server_time, long period) {

        long result = server_time + period;

        try {
            return new DateTime(result, DateTimeZone.UTC);
        } catch (Exception e) {
            LOGGER.error("Error while getting event date. ", e);
            return null;
        }
    }

    public static DateTime getEventTime(Long server_time) {

        try {
            return new DateTime(server_time, DateTimeZone.UTC);
        } catch (Exception e) {
            LOGGER.error("Error while getting event date. ", e);
            return null;
        }
    }

    public static JsonObject getJsonObject(String str) {

        try {
            return new Gson().fromJson(str, JsonObject.class);
        } catch (Exception e) {
            return new JsonObject();
        }
    }

    public static EventKey getEventKey(String str) {

        try {
            return new Gson().fromJson(str, EventKey.class);
        } catch (Exception e) {
            return new EventKey();
        }
    }

    public static void updatePushPeriod(int push_period, String key, JsonObject value, KeyValueStore<String, String> store) {
        value.addProperty("push_period", push_period);
        store.put(key, value.toString());
    }

    public static void updateScenarioId(int scenario_id, String key, JsonObject value,  KeyValueStore<String, String> store) {
        value.addProperty("scenario_id", scenario_id);
        store.put(key, value.toString());
    }

    public static boolean isPushTime(long eventTime, int push_period) {
        DateTime pushTime = ExecutorHelper.getPushTime(eventTime, push_period);
        DateTime nowTime = new DateTime(DateTimeZone.UTC);

        LOGGER.debug("now time: " + nowTime.toString());
        LOGGER.debug("push time: " + pushTime.toString());

        return nowTime.compareTo(pushTime) > 0;
    }

    public static int getPushPeriodFromEvent(JsonObject value, int initialPushPeriod) {
        return value.get("push_period").getAsInt() == 0 ? initialPushPeriod : value.get("push_period").getAsInt();
    }

    public static boolean eventTimeFits(EventKey conditionalEventKey, DateTime targetEventTime, KeyValueStore<String, String> store) {
        String conditionalEventValue = store.get(conditionalEventKey.toString());
        JsonObject conditionalEventValueJson = ExecutorHelper.getJsonObject(conditionalEventValue);

        DateTime conditionalEventTime = ExecutorHelper.getEventTime(conditionalEventValueJson.get("server_time").getAsLong());
        DateTime endOfDate = getEndOfDay(targetEventTime);

        return endOfDate.compareTo(conditionalEventTime) > 0;
    }

    public static boolean eventExists(EventKey conditionalEventKey, KeyValueStore<String, String> store) {
        String loginEventValue = store.get(conditionalEventKey.toString());
        return loginEventValue != null; // TODO проверить
    }

    public static boolean isCurrentDay(DateTime dateTime) {
        DateTime endOfDay = getEndOfDay(dateTime);
        DateTime nowTime = new DateTime(DateTimeZone.UTC);

        return endOfDay.compareTo(nowTime) > 0;
    }

    public static DateTime getEndOfDay(DateTime day) {
        return new LocalDateTime(day.toDate())
                .withHourOfDay(23)
                .withMinuteOfHour(59)
                .withSecondOfMinute(59)
                .toDateTime(DateTimeZone.UTC);
    }
}
