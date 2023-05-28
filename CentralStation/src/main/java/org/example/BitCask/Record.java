package org.example.BitCask;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Record {
    int keySize;
    int valSize;
    byte[] key;
    byte[] value;
    byte[] record;

    public Record(String key, String value) {
        //System.out.println(key+" "+value);
        this.key = key.getBytes();
        this.value = value.getBytes();
        this.keySize = this.key.length;
        this.valSize = this.value.length;
        this.record = new byte[8+keySize+valSize];
        System.arraycopy(ByteBuffer.allocate(4).putInt(keySize).array(),0,record,0,4);
        System.arraycopy(ByteBuffer.allocate(4).putInt(valSize).array(),0,record,4,4);
        System.arraycopy(this.key,0,record,8,keySize);
        System.arraycopy(this.value,0,record,8+keySize,valSize);

    }

    public void printRecord(){
        String k = new String(this.key);
        String v = new String(this.value);
        System.out.println(k+" "+v);
    }

    public byte[] getRecord(){
        return this.record;
    }

    public void writeRecord(String filePath){
        try {
            FileOutputStream toWrite = new FileOutputStream(filePath,true);
            toWrite.write(this.record);
            toWrite.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
