package org.example.Parquet;

import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ParquetInputHandler {
    private final HashMap<Long, List<GenericRecord>> stationsRecords;
    private final int batchSize;
    private final ParquetHandler parquetHandler;

    public ParquetInputHandler(int size) {
        batchSize = size;
        stationsRecords = new HashMap<>();
        parquetHandler = new ParquetHandler();
    }


    public void messagesConsumer(WeatherStationMessage weatherStationMessage,String parquetFilesPath) throws IOException {
        GenericRecord record = parquetHandler.createRecord(weatherStationMessage);
        List<GenericRecord> temp;
        if (stationsRecords.containsKey(weatherStationMessage.getStation_id())) {
            temp = stationsRecords.get(weatherStationMessage.getStation_id());
        } else {
            temp = new ArrayList<>();

        }
        temp.add(record);
        stationsRecords.put(weatherStationMessage.getStation_id(), temp);
        if(temp.size()>=batchSize){
            String fileName=parquetHandler.generateUniqueFilename();

            String path=parquetFilesPath+"/"+generatePath(weatherStationMessage.getStation_id())+"/"+fileName;
            System.out.println("fileName "+ path);
            parquetHandler.writeToParquetFile(path,temp);
            temp.clear();
        }

    }
    public String generatePath(long id) {
        String folder= LocalDate.now().toString();
        return folder+"/"+id;

    }

}
