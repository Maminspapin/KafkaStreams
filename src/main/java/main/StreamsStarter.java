package main;

import org.apache.kafka.streams.KafkaStreams;
import streams.EventStream;
import streams.ResultStream;

public class StreamsStarter {

    public static void main(String[] args) {

        // TODO добавить логирование
        // TODO добавить обработку исключений
        KafkaStreams eventStream = EventStream.newStream();
        KafkaStreams resultStream = ResultStream.newStream();

        eventStream.start();
        resultStream.start();
    }
}
