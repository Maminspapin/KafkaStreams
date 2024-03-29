package model;

public class EventKey {

    String user_id;
    int scenario_id;

    public EventKey() {

    }

    public EventKey(String user_id, int scenario_id) {
        this.user_id = user_id;
        this.scenario_id = scenario_id;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public int getScenario_id() {
        return scenario_id;
    }

    public void setScenario_id(int scenario_id) {
        this.scenario_id = scenario_id;
    }

    @Override
    public String toString() {
        return '{' +
                "\"user_id\": " + user_id +
                ", \"scenario_id\": " + scenario_id +
                '}';
    }
}
