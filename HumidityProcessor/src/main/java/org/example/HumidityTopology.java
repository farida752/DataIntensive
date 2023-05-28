package org.example;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.To;
import java.util.Properties;

public class HumidityTopology {

    static String inputTopic ="";
    static String outputTopic ="";
    private static class  HumidityMessage{
        int humidity;
        long station_id;
        String status_timestamp;

        public HumidityMessage(int humidity, long station_id, String status_timestamp) {
            this.humidity = humidity;
            this.station_id = station_id;
            this.status_timestamp = status_timestamp;
        }
        public String getMsg(){
            Gson gson = new Gson();
            System.out.println(gson.toJson(this));
            return gson.toJson(this);
        }
    }
    private static boolean isSpecificMessage(String message) {
        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(message, JsonObject.class);
        int humidity = jsonObject.get("weather").getAsJsonObject().get("humidity").getAsInt();
        return humidity >= 70;
    }

    private static String transformMessage(String message) {
        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(message, JsonObject.class);
        int humidity = jsonObject.get("weather").getAsJsonObject().get("humidity").getAsInt();
        String status_timestamp = jsonObject.get("status_timestamp").getAsString();
        long station_id = jsonObject.get("station_id").getAsLong();
        String newMsg = new HumidityMessage(humidity,station_id,status_timestamp).getMsg();
        System.out.println(newMsg);
        return newMsg;
    }
    public static Topology build() {
//        System.out.println("hello from topology build");
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic)
                .filter((key, value) -> isSpecificMessage((String) value))
                .mapValues((key, value) -> transformMessage((String) value))
                .to(outputTopic);

        return builder.build();

    }


    public static void main(String[] args) {
        inputTopic = System.getenv("input_topic");
        outputTopic = System.getenv("output_topic");
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde");

        Topology topology =  HumidityTopology.build();

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


}