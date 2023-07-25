package com.satvik.satvikdb.service.impl;

import com.satvik.satvikdb.context.Index;
import com.satvik.satvikdb.model.ByteOffset;
import com.satvik.satvikdb.service.WriteService;
import com.satvik.satvikdb.utils.GeneralUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;


public class SimpleWriteService implements WriteService {

    private final String dbFilePath;

    public SimpleWriteService(String dbFilePath) {
        this.dbFilePath = dbFilePath;
    }

    @Override
    public void write(String key, String value, Index simpleDbIndex){
        Path path = Paths.get(dbFilePath);
        byte[] bytes = GeneralUtils.getByteEncoding(key, value);
        try {
            long keyStartByte = Files.size(path); // new values will be written in EOF always
            ByteOffset byteOffset = GeneralUtils.getByteOffset(keyStartByte, key.length(), value.length());
            simpleDbIndex.put(key, byteOffset);
            Files.write(path, bytes, StandardOpenOption.APPEND);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void shutdown() {
        // no cleanup required
    }

}
