package org.example.Parquet;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class ParquetHandler {
    private static final Schema WEATHER_STATUS_SCHEMA = new Schema.Parser().parse(
            "{"
                    + "\"type\": \"record\","
                    + "\"name\": \"WeatherStatus\","
                    + "\"namespace\": \"com.example.weather\","
                    + "\"fields\": ["
                    + "{\"name\": \"station_id\", \"type\": \"long\"},"
                    + "{\"name\": \"s_no\", \"type\": \"long\"},"
                    + "{\"name\": \"battery_status\", \"type\": \"string\"},"
                    + "{\"name\": \"status_timestamp\", \"type\": \"long\"},"
                    + "{\"name\": \"humidity\", \"type\": \"int\"},"
                    + "{\"name\": \"temperature\", \"type\": \"int\"},"
                    + "{\"name\": \"wind_speed\", \"type\": \"int\"}"
                    + "]"
                    + "}");
    Schema schema = new Schema.Parser().parse(String.valueOf(WEATHER_STATUS_SCHEMA));

    public GenericRecord createRecord(WeatherStationMessage weatherStationMessage) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("station_id", weatherStationMessage.getStation_id());
        record.put("s_no", weatherStationMessage.getS_no());
        record.put("battery_status", weatherStationMessage.getBattery_status());
        record.put("status_timestamp", weatherStationMessage.getStatus_timestamp());
        record.put("humidity", weatherStationMessage.getWeather().getHumidity());
        record.put("temperature", weatherStationMessage.getWeather().getTemperature());
        record.put("wind_speed", weatherStationMessage.getWeather().getWindSpeed());
        return record;
    }
    public void writeToParquetFile(String filePath, List<GenericRecord> records) throws IOException {
        Configuration conf = new Configuration();
        ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(new org.apache.hadoop.fs.Path(filePath))
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();

        for (GenericRecord record : records) {
            writer.write(record);
        }
        writer.close();
    }

    public String generateUniqueFilename() {
        String baseFilename = "weather-status";
        // Get the current timestamp in milliseconds
        long timestamp = System.currentTimeMillis();
        // Format the timestamp as a string with the format "yyyyMMdd-HHmm ssSSS"
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmm ssSSS");
        String timestampStr = dateFormat.format(new Date(timestamp));
        // Concatenate the base filename and the timestamp string to form the unique filename
        return baseFilename + "-" + timestampStr + ".parquet";
    }


}
