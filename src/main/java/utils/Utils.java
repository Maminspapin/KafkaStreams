package utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import model.EventKey;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class Utils {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    public static DateTime getAfter (String server_time, long period) {

        long result = Long.valueOf(server_time) + period;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        try {
            String dateEvent = formatter.format(result);
            return DateTime.parse(dateEvent);
        } catch (Exception e) {
            LOGGER.error("Error while getting event date. ", e);
            return null;
        }
    }

    public static JsonObject getJsonObject(String str) {
        return new Gson().fromJson(str, JsonObject.class);
    }

    public static EventKey getEventKey(String str) {
        return new Gson().fromJson(str, EventKey.class);
    }
}
