package org.example.BitCask;

public class valueDirectory {
    String pathName;
    //read from start offset
    int start;

    //either read until end offset or read specific size
    //int end;
    int size;

    public valueDirectory(String fileName, int start, int size) {
        this.pathName = fileName;
        this.start = start;
        this.size = size;
    }
}
