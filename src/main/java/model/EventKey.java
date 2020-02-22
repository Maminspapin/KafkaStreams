package model;

public class EventKey {

    int user_id;
    int event_id;

    public EventKey() {

    }

    public EventKey(int user_id, int event_id) {
        this.user_id = user_id;
        this.event_id = event_id;
    }

    public int getUser_id() {
        return user_id;
    }

    public void setUser_id(int user_id) {
        this.user_id = user_id;
    }

    public int getEvent_id() {
        return event_id;
    }

    public void setEvent_id(int event_id) {
        this.event_id = event_id;
    }

    @Override
    public String toString() {
        return '{' +
                "\"user_id\": " + user_id +
                ", \"event_id\": " + event_id +
                '}';
    }
}
