package org.example;

import org.example.BitCask.BitcaskOperations;
import org.example.Parquet.ParquetInputHandler;
import org.example.Parquet.WeatherStationMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CentralStationHandler {
    private static Properties getKafkaProps() {
        // Set up Kafka consumer configuration
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "GROUP_ID");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }


    public static void kafkaConsumer() {
        Properties props = getKafkaProps();
        // Create Kafka consumer
        ObjectMapper mapper = new ObjectMapper();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        final String TOPIC = System.getenv("topic_name");
        final String PARQUET_PATH = System.getenv("parquet_files_path");
        final String BITCASK_PATH = System.getenv("bitcask_files_path");

        consumer.subscribe(Collections.singletonList(TOPIC));
        ParquetInputHandler parquetInputHandler = new ParquetInputHandler(10000);
        BitcaskOperations bcs = new BitcaskOperations(BITCASK_PATH,300);
        // Consume messages and write to org.example.Parquet files in batches
        while (true) {
            try {
                ConsumerRecords<String, String> recordsBatch = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : recordsBatch) {
                    // Parse the message into a GenericRecord
                    WeatherStationMessage message = mapper.readValue(record.value(), WeatherStationMessage.class);

                    parquetInputHandler.messagesConsumer(message,PARQUET_PATH);

                    bcs.addRecord(String.valueOf(message.getStation_id()),record.value());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) {
        kafkaConsumer();
    }

}
