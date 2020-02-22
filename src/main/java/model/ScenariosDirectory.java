package model;

public class ScenariosDirectory { // TODO использовать как мапу?

    int id;
    int category_id;
    int action_id;
    String description;

    public ScenariosDirectory() {
    }

    public ScenariosDirectory(int id, int category_id, int action_id, String description) {
        this.id = id;
        this.category_id = category_id;
        this.action_id = action_id;
        this.description = description;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return "ScenariosDirectory{" +
                "id=" + id +
                ", category_id=" + category_id +
                ", action_id=" + action_id +
                ", description='" + description + '\'' +
                '}';
    }
}
