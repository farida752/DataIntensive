package org.example.BitCask;

public interface IBitcask {
    public void addRecord(String key, String message);
    public String readCurrntRecords(String key);
    public void compression();
    public void rebuild();
    public void scheduleCompression(long interval);
}
