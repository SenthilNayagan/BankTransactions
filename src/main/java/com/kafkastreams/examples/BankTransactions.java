package com.kafkastreams.examples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class BankTransactions {
    public static void main(String[] args) {
        System.out.println("Bank Transactions");

        String sourceTopic = "source-topic";
        String sinkTopic = "sink-topic";

        Properties pros = getStreamsConfig();

        StreamsBuilder builder = new StreamsBuilder();

        // Creates KStream object from soureTopic
        // KStream represents stream of records
        KStream<String, String> sourceStream = builder.stream(
                sourceTopic,
                Consumed.with(
                        Serdes.String(),  // Key serde to consume message from source topic (Deserialization)
                        Serdes.String()  // Value serde to consume message from source topic (Deserialization)
                )
        );

        // Processor stream - Converts to uppercase
        KStream<String, String> upperCasedStream = sourceStream.mapValues(
                value -> value.toUpperCase()
        );

        // Printing processor stream on the console
        upperCasedStream.foreach(
                (key, value) -> System.out.println("Key: " + key + ", Value: " + value)
        );

        // Starting Kafka Stream job
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, pros);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Properties getStreamsConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "BankTransactions");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        return props;
    }
}
