package com.nebutown.persistency;

public interface ITable {
    void putRecord(String key, String jsonValue);

    String getRecord(String key);
}
