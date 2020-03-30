package main;

import org.apache.kafka.streams.KafkaStreams;
import streams.EventStream;
import streams.ResultStream;

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
