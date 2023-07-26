package com.satvik.satvikdb.service.impl;

import com.satvik.satvikdb.context.Memtable;
import com.satvik.satvikdb.model.DbFilePath;
import com.satvik.satvikdb.model.KeyValuePair;
import com.satvik.satvikdb.utils.GeneralUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class LsmWriteService {
    private String walPath;
    public LsmWriteService(String walPath) {
        this.walPath = walPath;
    }

    public void write(String key1, String value, Memtable memtable, LsmDb lsmDb) {
        String key = key1.replaceAll("\\s+", "_");
        try {
            Files.write(Path.of(walPath), GeneralUtils.getSimpleByteEncoding(key, value), StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        memtable.put(key, value);
        if(memtable.isThresholdPassed()){
            // write memtable to dbFilePath and indexFilePath
            System.out.println("threshold of "+memtable.getThreshold()+" is passed. Making new files");
            DbFilePath dbFilePath = LsmDb.createNewFiles();
            dumpMemtable(memtable, dbFilePath);
            lsmDb.reset();
        }
    }

    public void dumpMemtable(Memtable memtable, DbFilePath dbFilePath) {
        long startSize;
        try {
            startSize = Files.size(Path.of(dbFilePath.getDbFilePath()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<KeyValuePair> data = new ArrayList<>();
        memtable.forEach((key, value) -> data.add(new KeyValuePair(key, value)));

        GeneralUtils.writeToDisk(dbFilePath, startSize, data);
    }
}
