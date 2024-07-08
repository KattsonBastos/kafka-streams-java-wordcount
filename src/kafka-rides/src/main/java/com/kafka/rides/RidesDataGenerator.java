package com.kafka.rides;

import com.github.javafaker.Faker;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class RidesDataGenerator {

    private static final String RIDES_FILE_LOCATION = "src/main/resources/rides.csv";
    private final Faker faker = new Faker();
    private final Random random = new Random();

    public List<Map<String, Object>> getMultipleRows(int genDtRows) {
        List<Map<String, Object>> ridesData = new ArrayList<>();
        String formattedTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());

        try (CSVReader reader = new CSVReader(new FileReader(RIDES_FILE_LOCATION))) {
            List<String[]> csvData = reader.readAll();
            String[] header = csvData.remove(0);
            
            Collections.shuffle(csvData);
            csvData = csvData.subList(0, genDtRows);

            for (String[] row : csvData) {
                Map<String, Object> ride = new HashMap<>();
                for (int i = 0; i < header.length; i++) {
                    ride.put(header[i].trim().toLowerCase().replace(" ", "_").replace("(", "").replace(")", ""), row[i]);
                }
                ride.put("user_id", faker.number().numberBetween(0, 1000));
                ride.put("vehicle_id", faker.number().numberBetween(0, 8219));
                ride.put("dt_current_timestamp", formattedTimestamp);
                if (ride.get("price") == null || ((String) ride.get("price")).isEmpty()) {
                    ride.put("price", 0);
                }
                ridesData.add(ride);
            }
        } catch (IOException | CsvException e) {
            e.printStackTrace();
        }

        return ridesData;
    }

    public static void main(String[] args) {
        RidesDataGenerator generator = new RidesDataGenerator();
        List<Map<String, Object>> rides = generator.getMultipleRows(10);
        rides.forEach(System.out::println);
    }
}
