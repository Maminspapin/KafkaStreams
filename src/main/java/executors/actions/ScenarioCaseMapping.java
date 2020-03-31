package executors.actions;

import java.util.HashMap;
import java.util.Map;

public class ScenarioCaseMapping {

    private static Map<Integer, String> scenarioMap = new HashMap<>();

    static {
        scenarioMap.put(11, "SCENARIO_11");
        scenarioMap.put(1100, "SCENARIO_1100");
        scenarioMap.put(1300, "SCENARIO_1300");
        scenarioMap.put(1500, "SCENARIO_1500");
        scenarioMap.put(21, "SCENARIO_21");
    }

    public static String getScenarioCase(int scenario_id) { //TODO посмотреть как работает класс java.util.Optional + постараться убрать этот код
        return scenarioMap.get(scenario_id);
    }
}
