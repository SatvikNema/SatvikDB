package com.satvik.satvikdb.service.impl;

import com.satvik.satvikdb.context.Index;
import com.satvik.satvikdb.service.DbService;
import com.satvik.satvikdb.service.ReadService;
import com.satvik.satvikdb.service.WriteService;
import com.satvik.satvikdb.utils.GeneralUtils;
import java.io.File;

public class SimpleDb extends DbService {

  private ReadService readService;
  private WriteService writeService;
  private Index index;

  private String dbFilePath;

  private String indexFilePath;

  @Override
  public void init(String rootDir, String dbFileName, String indexFileName, String walName) {
    GeneralUtils.createDirsIfNotExists(rootDir);

    this.dbFilePath = rootDir + File.separatorChar + dbFileName;
    this.indexFilePath = rootDir + File.separatorChar + indexFileName;
    GeneralUtils.createFileIfNotExists(dbFilePath);
    GeneralUtils.createFileIfNotExists(indexFilePath);

    this.index = GeneralUtils.loadIndex(indexFilePath);
    readService = new SimpleReadService(dbFilePath);
    writeService = new SimpleWriteService(dbFilePath);
  }

  @Override
  public String read(String key) {
    return readService.read(key, index);
  }

  @Override
  public void write(String key, String value) {
    writeService.write(key, value, index);
  }

  @Override
  public void shutdown() {
    GeneralUtils.saveIndex(indexFilePath, index);
    readService.shutdown();
    writeService.shutdown();
  }
}
