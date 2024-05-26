package com.satvik.satvikdb.service;

public abstract class DbService {

  public abstract void init(
      String rootDir, String dbFileName, String indexFileName, String walName);

  public abstract String read(String key);

  public abstract void write(String key, String value);

  public abstract void shutdown();
}
