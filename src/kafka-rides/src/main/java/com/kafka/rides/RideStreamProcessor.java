package com.kafka.rides;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Instant;
import java.util.Properties;

public class RideStreamProcessor {

    public static void main(String[] args) {

        String sourceTopic = "src-streaming-rides";
        String sinkTopic = "snk-rides-refined";

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ride-stream-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> rides = builder.stream(sourceTopic);

        KStream<String, String> processedRides = rides
                .mapValues(value -> {
                    JsonObject jsonObject = JsonParser.parseString(value).getAsJsonObject();

                    // Add processing time
                    jsonObject.addProperty("processing_time", Instant.now().toString());

                    // Convert distance from miles to kilometers
                    double distanceInMiles = jsonObject.get("distance").getAsDouble();
                    double distanceInKm = distanceInMiles * 1.60934;
                    jsonObject.addProperty("distance_km", distanceInKm);
                    
                    // Remove the old distance field
                    jsonObject.remove("distance");

                    String processedValue = jsonObject.toString();
                    System.out.println("Processed Value: " + processedValue);
                    return processedValue;
                })
                .filter((key, value) -> {
                    JsonObject jsonObject = JsonParser.parseString(value).getAsJsonObject();
                    double price = jsonObject.get("price").getAsDouble();
                    boolean isValid = price > 0;
                    if (isValid) {
                        System.out.println("Valid Message: " + value);
                    } else {
                        System.out.println("Invalid Message (Filtered Out): " + value);
                    }
                    return isValid;
                });

        processedRides.to(sinkTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
