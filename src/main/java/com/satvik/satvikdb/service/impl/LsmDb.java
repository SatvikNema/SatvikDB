package com.satvik.satvikdb.service.impl;

import com.satvik.satvikdb.bloomfilter.BloomFilter;
import com.satvik.satvikdb.context.Memtable;
import com.satvik.satvikdb.model.DbFilePath;
import com.satvik.satvikdb.service.DbService;
import com.satvik.satvikdb.utils.GeneralUtils;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class LsmDb extends DbService {
  private LsmReadService lsmReadService;
  private LsmWriteService writeService;
  private String dbFilePath;
  private String indexFilePath;
  private static String dbFileNameStartsWith;
  private static String indexFileNameStartsWith;
  private static String bloomFilterPathStartsWith;

  private static String rootDir;

  private String walPath;
  private Memtable memtable;
  private BloomFilter<String> bloomFilterInMemory;

  @Override
  public void init(
      String rootDir, String dbFileNameStartsWith, String indexFileNameStartsWith, String walName) {
    LsmDb.rootDir = rootDir;
    LsmDb.dbFileNameStartsWith = dbFileNameStartsWith;
    LsmDb.indexFileNameStartsWith = indexFileNameStartsWith;
    LsmDb.bloomFilterPathStartsWith = "bloom_filter_";

    GeneralUtils.createDirsIfNotExists(rootDir);
    walPath = rootDir + File.separatorChar + walName;
    GeneralUtils.createFileIfNotExists(walPath);

    memtable = new Memtable(100_000);
    // todo if wal is non empty, load content from wal into memtable
    this.bloomFilterInMemory =
        BloomFilter.create(431328, 10); // for ~30_000 elements with 0.001 error rate

    writeService = new LsmWriteService(walPath);
    lsmReadService =
        new LsmReadService(
            rootDir, dbFileNameStartsWith, indexFileNameStartsWith, bloomFilterPathStartsWith);
  }

  @Override
  public String read(String key) {
    return lsmReadService.get(key, memtable);
  }

  @Override
  public void write(String key, String value) {
    writeService.write(key, value, memtable, bloomFilterInMemory, this);
  }

  @Override
  public void shutdown() {
    if (memtable.getSize() > 0) {
      System.out.println("memtable is non empty. Dumping remaining entries to new files.");
      DbFilePath dbFilePath = createNewFiles();
      writeService.dumpMemtable(memtable, dbFilePath, bloomFilterInMemory);
      reset();
    }
  }

  public void reset() {
    memtable = new Memtable(memtable.getThreshold());
    clearWal();
    lsmReadService.setNewFileAdded(true);
    this.bloomFilterInMemory = BloomFilter.create(431328, 10);
  }

  private void clearWal() {
    try {
      FileChannel.open(Paths.get(walPath), StandardOpenOption.WRITE).truncate(0).close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static DbFilePath createNewFiles() {
    long currentEpoch = System.currentTimeMillis();
    String dbFilePath = rootDir + File.separatorChar + dbFileNameStartsWith + currentEpoch;
    String indexFilePath = rootDir + File.separatorChar + indexFileNameStartsWith + currentEpoch;
    String bloomFilterPath =
        rootDir + File.separatorChar + bloomFilterPathStartsWith + currentEpoch;
    GeneralUtils.createFiles(dbFilePath, indexFilePath, bloomFilterPath);
    return new DbFilePath(dbFilePath, indexFilePath, bloomFilterPath);
  }

  public static DbFilePath createNewFiles(
      String rootDir, String indexFileNameStartsWith, String dbFileNameStartsWith) {
    long currentEpoch = System.currentTimeMillis();
    String dbFilePath = rootDir + File.separatorChar + dbFileNameStartsWith + currentEpoch;
    String indexFilePath = rootDir + File.separatorChar + indexFileNameStartsWith + currentEpoch;
    GeneralUtils.createFiles(dbFilePath, indexFilePath);
    return new DbFilePath(dbFilePath, indexFilePath);
  }
}
