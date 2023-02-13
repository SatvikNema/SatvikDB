package com.satvik.satvikdb.service.impl;

import com.satvik.satvikdb.context.Memtable;
import com.satvik.satvikdb.model.DbFilePath;
import com.satvik.satvikdb.service.DbService;
import com.satvik.satvikdb.service.ReadService;
import com.satvik.satvikdb.service.WriteService;
import com.satvik.satvikdb.utils.GeneralUtils;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class LsmDb extends DbService {
    private LsmReadService lsmReadService;
    private LsmWriteService writeService;
    private String dbFilePath;
    private String indexFilePath;
    private static String dbFileNameStartsWith;
    private static String indexFileNameStartsWith;

    private static String rootDir;

    private String walPath;
    private Memtable memtable;
    @Override
    public void init(String rootDir, String dbFileNameStartsWith, String indexFileNameStartsWith, String walName) {
        LsmDb.rootDir = rootDir;
        LsmDb.dbFileNameStartsWith = dbFileNameStartsWith;
        LsmDb.indexFileNameStartsWith = indexFileNameStartsWith;

        GeneralUtils.createDirsIfNotExists(rootDir);
        walPath = rootDir + File.separatorChar + walName;
        GeneralUtils.createFileIfNotExists(walPath);

        memtable = new Memtable(1000);
        // TODO if wal is non empty, load content from wal into memtable

        writeService = new LsmWriteService(walPath);
        lsmReadService = new LsmReadService(rootDir, dbFileNameStartsWith, indexFileNameStartsWith);
    }

    @Override
    public String read(String key) {
        return lsmReadService.get(key, memtable);
    }

    @Override
    public void write(String key, String value) {
        writeService.write(key, value, memtable, this);
    }

    @Override
    public void shutdown() {
        if(memtable.getSize() > 0){
            System.out.println("memtable is non empty. Dumping remaining entries to new files.");
            DbFilePath dbFilePath = createNewFiles();
            writeService.dumpMemtable(memtable, dbFilePath);
            reset();
        }
    }

    public void reset(){
        memtable = new Memtable(memtable.getThreshold());
        clearWal();
        lsmReadService.setNewFileAdded(true);
    }

    private void clearWal() {
        try {
            FileChannel.open(Paths.get(walPath), StandardOpenOption.WRITE).truncate(0).close();
            System.out.println("wal is cleared");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static DbFilePath createNewFiles() {
        long currentEpoch = System.currentTimeMillis();
        String dbFilePath = rootDir+File.separatorChar + dbFileNameStartsWith + currentEpoch;
        String indexFilePath = rootDir+File.separatorChar + indexFileNameStartsWith + currentEpoch;
        GeneralUtils.createFiles(dbFilePath, indexFilePath);
        return new DbFilePath(dbFilePath, indexFilePath);
    }

    public static DbFilePath createNewFiles(String rootDir, String indexFileNameStartsWith, String dbFileNameStartsWith) {
        long currentEpoch = System.currentTimeMillis();
        String dbFilePath = rootDir+File.separatorChar + dbFileNameStartsWith + currentEpoch;
        String indexFilePath = rootDir+File.separatorChar + indexFileNameStartsWith + currentEpoch;
        GeneralUtils.createFiles(dbFilePath, indexFilePath);
        return new DbFilePath(dbFilePath, indexFilePath);
    }

}
