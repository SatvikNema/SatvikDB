package com.satvik.satvikdb.service.impl;

import com.satvik.satvikdb.context.Index;
import com.satvik.satvikdb.model.ByteOffset;
import com.satvik.satvikdb.model.FileEntry;
import com.satvik.satvikdb.service.ReadService;
import com.satvik.satvikdb.utils.GeneralUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.InvalidPathException;


public class SimpleReadService implements ReadService {

    private final String dbFilePath;

    public SimpleReadService(String dbFilePath) {
        this.dbFilePath = dbFilePath;
    }

    public String read(String key, Index simpleDbIndex) {
        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(dbFilePath, "r");
        } catch (FileNotFoundException e) {
            throw new InvalidPathException(dbFilePath, e.toString());
        }

        if(!simpleDbIndex.containsKey(key)){
            System.out.println(key +" is not present in index. Will lookup db file");
            FileEntry fileEntry = GeneralUtils.fetchValueFromFile(raf, key).orElseThrow();
            try {
                raf.close();
            } catch (IOException e) {
                System.out.println("error occurred while closing the file");
            }
            // update the in memory cache
            simpleDbIndex.put(fileEntry.getKeyValuePair().getKey(), fileEntry.getByteOffset());
            return fileEntry.getKeyValuePair().getValue();
        }

        ByteOffset byteOffset = simpleDbIndex.get(key);
        String value = GeneralUtils.getValueFromByteOffset(raf, byteOffset);
        return value;
    }

    @Override
    public void shutdown() {
        // no cleanup required
    }
}
