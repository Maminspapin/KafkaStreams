package config;

public enum Topics {

    MATOMO_LOG_LINK_VISIT_ACTION {
        @Override
        public String toString() { return "matomo.matomo.matomo_log_link_visit_action"; }
        //public String toString() { return "matomo.matomo.visit"; }
    },
    MATOMO_SCENARIOS_DETAIL {
        @Override
        public String toString() { return "matomo.matomo.matomo_scenarios_detail"; }
        //public String toString() { return "matomo.matomo.scenario"; }
    },
    EVENTS {
        @Override
        public String toString() { return "events"; }
    },
    RESULTS {
        @Override
        public String toString() { return "results"; }
    };

    public String topicName() { return this.toString(); }

}
