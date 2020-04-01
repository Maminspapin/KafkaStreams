package utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import model.EventKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    public static DateTime getPushTime(long server_time, long period) { // TODO поменять на long

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
        return new Gson().fromJson(str, EventKey.class);
    }
}
