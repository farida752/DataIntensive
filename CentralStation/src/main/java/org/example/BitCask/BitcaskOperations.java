package org.example.BitCask;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BitcaskOperations implements IBitcask{
    HashMap<String, valueDirectory> memtable;
    String currentFile = "current.data";
    String directoryName;
    HashMap<String, valueDirectory> mergeTable;
    int maxSize = 2000;

    public BitcaskOperations(String directoryPath, long interval) {
        this.directoryName = directoryPath;
        if(Files.exists(Paths.get(directoryName))){
            System.out.println("dir exists");
            if(!Files.exists(Paths.get(directoryPath+File.separator+currentFile))){
                System.out.println("dir file doesn't exists");
                try {
                    Files.createFile(Paths.get(directoryName+File.separator+currentFile));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }else{
            try {
                Files.createDirectory(Paths.get(directoryName));
                Files.createFile(Paths.get(directoryName+File.separator+currentFile));
                System.out.println("dir doesn't exist");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        this.memtable = new HashMap<>();
        this.mergeTable = new HashMap<>();
        rebuild();
        scheduleCompression(interval);
    }

    //add record using station key and message
    @Override
    public void addRecord(String key, String message){
        System.out.println("add record");
        Record record = new Record(key,message);
        int fileSize = 0;
        try {
            fileSize = (int) Files.size(Paths.get(directoryName+File.separator+currentFile));
            if(fileSize+record.getRecord().length>maxSize){
                renameAndCreateCurrent();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        memtable.put(key,new valueDirectory(currentFile,
                fileSize,record.getRecord().length));
        record.writeRecord(directoryName+File.separator+currentFile);
    }

    //if current file filled, rename it with current time and create new current file to write to
    private void renameAndCreateCurrent(){
        File oldFile = new File(directoryName+File.separator+currentFile);
        File newFile = new File(directoryName+File.separator+System.currentTimeMillis()+".data");
        while(Files.exists(newFile.toPath())){
            newFile = new File(directoryName+File.separator+System.currentTimeMillis()+".data");
        }
        oldFile.renameTo(newFile);
        try {
          Files.createFile(Paths.get(directoryName+File.separator+currentFile));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //read record of memtable key from current file
    @Override
    public String readCurrntRecords(String key){
        return readRecord(key,true);
    }

    //read record of memtable key from compressed file
    private String readCompressedRecord(String key){
        return readRecord(key,false);
    }

    //read record from any file depending on key
    private String readRecord(String key, boolean memT){
        HashMap<String, valueDirectory> used;
        if(memT){
            used = memtable;
        }else {
            used = mergeTable;
        }
        if(used.containsKey(key)){
            valueDirectory location = used.get(key);
            try {
                RandomAccessFile toRead = new RandomAccessFile(directoryName+File.separator+location.pathName,"r");
                toRead.seek(location.start);
                byte[] readIn = new byte[location.size];
                toRead.read(readIn,0,location.size);
                toRead.close();
                int keysize = ByteBuffer.wrap(readIn,0,4).getInt();
                int valsize = ByteBuffer.wrap(readIn,4,4).getInt();
                String keyVal = new String(Arrays.copyOfRange(readIn,8,8+keysize));
                String value = new String(Arrays.copyOfRange(readIn,8+keysize,readIn.length));
                return value;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }else{
            return "Key doesn't exist";
        }
    }

    @Override
    public void scheduleCompression(long interval){
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(()->{
           compression();
        },interval,interval, TimeUnit.SECONDS);
    }

    //Compaction
    @Override
    public void compression(){
        mergeTable = new HashMap<>();
        File dir = new File(directoryName);

        //list of files except current file
        File[] oldFiles = dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return !pathname.getName().contains("current");
            }
        });
        Arrays.sort(oldFiles, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                long first = Long.parseLong(o1.getName().split("\\.")[0]);
                long second = Long.parseLong(o2.getName().split("\\.")[0]);
                return (int) (second-first);
            }
        });

        //process hint files and data files that has no hint file (not compressed and not current file)
        if(oldFiles.length<2 || (oldFiles.length==2 && oldFiles[1].getName().contains("hint"))){
            return;
        }
        for(int i=0; i<oldFiles.length; i++){
            if(i!=oldFiles.length-1 && (oldFiles[i].getName().split("\\.")[0].equals(oldFiles[i+1].getName().split("\\.")[0]))){
                i = i+1;
            }
            if(oldFiles[i].getName().contains("hint")){
                processHintFile(oldFiles[i]);
            }else{
                processDataFile(oldFiles[i]);
            }
        }

        //create new compressed file to write record to
        String compressName = System.currentTimeMillis()+".data";
        try {
            while(Files.exists(Paths.get(directoryName+File.separator+compressName))){
                compressName = System.currentTimeMillis()+".data";
            }
            Files.createFile(Paths.get(directoryName+File.separator+compressName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        File compressed = new File(directoryName+File.separator+compressName);

        //write records to new compressed file and update mergeTable with new file name and new offset
        for(String key: mergeTable.keySet()){
            String val = readRecord(key,false);
            Record record = new Record(key,val);
            valueDirectory newFilePosition = new valueDirectory(compressName,(int)(compressed.length()),record.record.length);
            mergeTable.put(key,newFilePosition);
            record.writeRecord(directoryName+File.separator+compressName);
        }

        //create hint file with values in mergeTable
        createHintFile(compressName);

        //delete old files that was used in compression except current file
        for(File f: oldFiles){
            f.delete();
        }

        //add compressed values to current hashmap with values of current file hashmap
        rehashMerge();
    }

    //value not record read but position of records same as memtable
    //then when write, open file get record as position then read record and write in new file
    private void processDataFile(File fileName){
        byte[] readIn = new byte[(int)fileName.length()];
        try {
            FileInputStream toRead = new FileInputStream(fileName);
            toRead.read(readIn);
            toRead.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for(int i=0; i<readIn.length; i++){
            int keysize = ByteBuffer.wrap(readIn,i,4).getInt();
            int valsize = ByteBuffer.wrap(readIn,i+4,4).getInt();
            String keyVal = new String(Arrays.copyOfRange(readIn,i+8,i+8+keysize));
            if(mergeTable.containsKey(keyVal)){
                if(mergeTable.get(keyVal).pathName.equals(fileName.getName())){
                    valueDirectory val = new valueDirectory(fileName.getName(),i,8+keysize+valsize);
                    mergeTable.put(keyVal,val);
                }
            }else{
                valueDirectory val = new valueDirectory(fileName.getName(),i,8+keysize+valsize);
                mergeTable.put(keyVal,val);
            }
            i = i+8+keysize+valsize-1;
        }

    }

    //has key size, start, record size, key
    //what is in valueDirectory
    private void processHintFile(File fileName){
        byte[] readIn = new byte[(int)fileName.length()];
        try {
            FileInputStream toRead = new FileInputStream(fileName);
            toRead.read(readIn);
            toRead.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for(int i=0; i<readIn.length; i++){
            int keySize = ByteBuffer.wrap(readIn,i,4).getInt();
            int offset = ByteBuffer.wrap(readIn,i+4,4).getInt();
            int recSize = ByteBuffer.wrap(readIn,i+8,4).getInt();
            String keyVal = new String(Arrays.copyOfRange(readIn,i+12,i+12+keySize));
            if(!mergeTable.containsKey(keyVal)){
                mergeTable.put(keyVal,new valueDirectory(fileName.getName().replace("hint","data"),offset,recSize));
            }
            i = i+12+keySize-1;
        }
    }

    //create hint file (keysize,offset,size of record, key)
    private void createHintFile(String compressName){
        compressName = compressName.replace("data","hint");
        try {
            Files.createFile(Paths.get(directoryName+File.separator+compressName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for(String key: mergeTable.keySet()){
            byte[] keyBytes = key.getBytes();
            byte[] hintRecord = new byte[12+keyBytes.length];
            valueDirectory get = mergeTable.get(key);
            System.arraycopy(ByteBuffer.allocate(4).putInt(keyBytes.length).array(),0,hintRecord,0,4);
            System.arraycopy(ByteBuffer.allocate(4).putInt(get.start).array(),0,hintRecord,4,4);
            System.arraycopy(ByteBuffer.allocate(4).putInt(get.size).array(),0,hintRecord,8,4);
            System.arraycopy(keyBytes,0,hintRecord,12,keyBytes.length);
            try {
                FileOutputStream toWrite = new FileOutputStream(directoryName+File.separator+compressName,true);
                toWrite.write(hintRecord);
                toWrite.close();
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    //rehash from mergeTable
    private void rehashMerge(){
        for(String key: mergeTable.keySet()){
            if(!memtable.containsKey(key)){
                memtable.put(key,mergeTable.get(key));
            }
        }
        mergeTable = new HashMap<>();
    }

    //rebuild memtable when first open program
    @Override
    public void rebuild(){
        memtable = new HashMap<>();
        mergeTable = new HashMap<>();
        File dir = new File(directoryName);
        File[] hintFiles = dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().contains("hint");
            }
        });
        Arrays.sort(hintFiles, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                long first = Long.parseLong(o1.getName().split("\\.")[0]);
                long second = Long.parseLong(o2.getName().split("\\.")[0]);
                return (int) (second-first);
            }
        });
        for(File hint: hintFiles){
            processHintFile(hint);
        }
        //process whole current file
        rehashCurrentFile();
        rehashMerge();
    }

    //rehash from current file
    private void rehashCurrentFile(){
        File curr = new File(directoryName+File.separator+currentFile);
        byte[] readIn = new byte[(int)curr.length()];
        try {
            FileInputStream toRead = new FileInputStream(curr);
            toRead.read(readIn);
            toRead.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for(int i=0; i<readIn.length; i++){
            int keysize = ByteBuffer.wrap(readIn,i,4).getInt();
            int valsize = ByteBuffer.wrap(readIn,i+4,4).getInt();
            String keyVal = new String(Arrays.copyOfRange(readIn,i+8,i+8+keysize));
            memtable.put(keyVal,new valueDirectory(currentFile,i,8+keysize+valsize));
            i = i+8+keysize+valsize-1;
        }
    }

}
