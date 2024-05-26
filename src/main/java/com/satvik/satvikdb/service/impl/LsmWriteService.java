package com.satvik.satvikdb.service.impl;

import com.satvik.satvikdb.bloomfilter.BloomFilter;
import com.satvik.satvikdb.context.FeatureFlags;
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

    public void write(String key1, String value, Memtable memtable, BloomFilter<String> bloomFilter, LsmDb lsmDb) {
        String key = key1.replaceAll("\\s+", "_");
        try {
            Files.write(Path.of(walPath), GeneralUtils.getSimpleByteEncoding(key, value), StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        memtable.put(key, value);
        if(FeatureFlags.BLOOM_FILTERS_ENABLED) {
            bloomFilter.add(key);
        }
        if(memtable.isThresholdPassed()){
            // write memtable to dbFilePath and indexFilePath
            DbFilePath dbFilePath = LsmDb.createNewFiles();
            dumpMemtable(memtable, dbFilePath, bloomFilter);
            lsmDb.reset();
        }
    }

    public void dumpMemtable(Memtable memtable, DbFilePath dbFilePath, BloomFilter<String> bloomFilter) {
        long startSize;
        try {
            startSize = Files.size(Path.of(dbFilePath.getDbFilePath()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<KeyValuePair> data = new ArrayList<>();
        memtable.forEach((key, value) -> data.add(new KeyValuePair(key, value)));

        GeneralUtils.writeToDisk(dbFilePath, startSize, data);

        if(FeatureFlags.BLOOM_FILTERS_ENABLED) {
            try {
                bloomFilter.serialise(dbFilePath.getBloomFilterPath());
            } catch (IOException e) {
                System.out.println("failed while saving bloom filter disk!");
            }
        }
    }
}
