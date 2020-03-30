package executors.action;

import java.util.HashMap;
import java.util.Map;

public class ScenarioCaseMapping {

    private static Map<Integer, String> scenarioMap = new HashMap<>();

    static {
        scenarioMap.put(11, "SCENARIO_11");
        scenarioMap.put(1100, "SCENARIO_1100");
        scenarioMap.put(1300, "SCENARIO_1300");
    }

    public static String getScenarioCase(int scenario_id) { //TODO посмотреть как работает класс java.util.Optional + постараться убрать этот код
        return scenarioMap.get(scenario_id);
    }
}
