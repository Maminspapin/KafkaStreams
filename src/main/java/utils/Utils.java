package utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import model.EventKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Utils {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    public static Date getAfter (String server_time, long period) {

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        Date dateEvent = null;
        try {
            dateEvent = formatter.parse(server_time);
        } catch (ParseException e) {
            LOGGER.error("Error while getting event date. ", e);
        }

        return new Date(dateEvent.getTime() + period);
    }

    public static JsonObject getJsonObject(String str) {
        return new Gson().fromJson(str, JsonObject.class);
    }

    public static EventKey getEventKey(String str) {
        return new Gson().fromJson(str, EventKey.class);
    }
}
