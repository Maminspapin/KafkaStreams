package main;

import executors.ScenarioExecutor;
import org.apache.kafka.streams.KafkaStreams;
import streams.EventStream;
import streams.ResultStream;
import streams.ScenarioStream;

public class StreamsStarter {

    public static void main(String[] args) {

        //KafkaStreams scenarioStream = ScenarioStream.newStream();
        KafkaStreams eventStream = EventStream.newStream();
        KafkaStreams resultStream = ResultStream.newStream();

        //scenarioStream.start();
        eventStream.start();
        resultStream.start();
    }
}
