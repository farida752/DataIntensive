package org.example;

//import util.properties packages
import java.util.Properties;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.UUID;
import org.json.*;
import com.google.gson.Gson;


//Create java class named “SimpleProducer”
public class WeatherStation {

    public static void main(String[] args) throws Exception{

//        //Assign topicName to string variable
//        String topicName = "weather-events";
//        UUID uuid = UUID.randomUUID();
//        long mostSignificantBits = uuid.getMostSignificantBits();
//        long leastSignificantBits = uuid.getLeastSignificantBits();
//        long stationId = (mostSignificantBits << 64) | leastSignificantBits;
//        System.out.println(stationId);
        long stationId = Long.parseLong(System.getenv("station_id"));
        String topicName = System.getenv("topic_name");
        long sNo = 1;
        int totalMsgs = 10000;

        // create instance for properties to access producer configs
        Properties props = new Properties();
        //Assign localhost id
        props.put("bootstrap.servers", "kafka:9092");
        //Set acknowledgements for producer requests.
        props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        //Specify buffer size in config
//        props.put("batch.size", 10);
        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
//        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        for(int i = 0; i < totalMsgs; i++){
            String message = new WeatherMsgAdapter(stationId,sNo).getMsg();
            sNo++;
            if(i % 10 != 0) {
                producer.send(new ProducerRecord<String,String>(topicName,message));
                System.out.println("Message sent successfully");
            }
            else
            System.out.println("Message dropped");

            Thread.sleep(1000);
        }
        producer.close();
    }
}
