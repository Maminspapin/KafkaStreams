package model;

public class JoinKey {

    int category_id;
    int action_id;

    public JoinKey() {
    }

    public JoinKey(int action_id, int category_id) {
        this.action_id = action_id;
        this.category_id = category_id;
    }

    public int getCategory_id() {
        return category_id;
    }

    public void setCategory_id(int category_id) {
        this.category_id = category_id;
    }

    public int getAction_id() {
        return action_id;
    }

    public void setAction_id(int action_id) {
        this.action_id = action_id;
    }

    @Override
    public String toString() {
        return '{' +
                "\"action_id\": " + action_id +
                ", \"category_id\": " + category_id +
                '}';
    }
}
