package com.kafka.rides;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class RideDataProducer {

    private static final String TOPIC = "src-streaming-rides";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final Gson gson = new GsonBuilder().create();

    private static final int ROWCOUNT = 100;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        RidesDataGenerator generator = new RidesDataGenerator();

        while (true) {

            long startTime = System.currentTimeMillis(); // Start time

            List<Map<String, Object>> rides = generator.getMultipleRows(ROWCOUNT);

            for (Map<String, Object> ride : rides) {
                String jsonString = gson.toJson(ride);
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, jsonString);
                producer.send(record);

                // System.out.println("Sent: " + jsonString);
            }
            producer.flush();
            // producer.close();
            // Thread.sleep(1000);

            long endTime = System.currentTimeMillis(); // End time

            long duration = endTime - startTime; // Duration in milliseconds
            System.out.println("Time taken to send " + ROWCOUNT + " messages: " + duration + " ms");

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }
}
