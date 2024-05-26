package com.satvik.satvikdb.service.impl;

import com.satvik.satvikdb.bloomfilter.BloomFilter;
import com.satvik.satvikdb.context.FeatureFlags;
import com.satvik.satvikdb.context.Index;
import com.satvik.satvikdb.context.Memtable;
import com.satvik.satvikdb.model.ByteOffset;
import com.satvik.satvikdb.model.DbFilePath;
import com.satvik.satvikdb.utils.GeneralUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class LsmReadService {
  private String dbFilePathStartsWith;
  private String indexFilePathStartsWith;
  private String bloomFilterPathStartsWith;

  private String rootDir;

  private List<DbFilePath> diskLookups;

  private boolean newFileAdded;

  private Map<String, BloomFilter<String>> bloomFilterMap;

  public LsmReadService(
      String rootDir,
      String dbFilePathStartsWith,
      String indexFilePathStartsWith,
      String bloomFilterPathStartsWith) {
    this.dbFilePathStartsWith = dbFilePathStartsWith;
    this.indexFilePathStartsWith = indexFilePathStartsWith;
    this.bloomFilterPathStartsWith = bloomFilterPathStartsWith;
    this.rootDir = rootDir;
    diskLookups =
        GeneralUtils.loadFilesSortedByAgeDesc(
            rootDir, indexFilePathStartsWith, dbFilePathStartsWith, bloomFilterPathStartsWith);
    newFileAdded = false;
    bloomFilterMap = new HashMap<>();
  }

  public String get(String key, Memtable memtable) {

    if (memtable.containsKey(
        key)) { // no need of bloom filter lookup here. This is already a ~O(1) operation
      return memtable.get(key);
    }

    if (newFileAdded) {
      // new writes were added before last lookup. refreshing diskLookups entity
      diskLookups =
          GeneralUtils.loadFilesSortedByAgeDesc(
              rootDir, indexFilePathStartsWith, dbFilePathStartsWith, bloomFilterPathStartsWith);
      newFileAdded = false;
    }

    Optional<String> optionalValue = findValueInFiles(key, diskLookups);
    return optionalValue.orElse(null);
  }

  private Optional<String> findValueInFiles(String key, List<DbFilePath> diskLookups) {
    String value;
    Optional<String> result = Optional.empty();
    for (DbFilePath dbFilePath : diskLookups) {
      value = findValueInFile(key, dbFilePath);
      if (value != null) {
        result = Optional.of(value);
        break;
      }
    }
    return result;
  }

  private String findValueInFile(String key, DbFilePath dbFilePath) {
    if (!isElementInserted(key, dbFilePath.getBloomFilterPath())) {
      // early exit
      return null;
    }
    String indexPath = dbFilePath.getIndexFilePath();
    Index index = GeneralUtils.loadIndex(indexPath);
    if (index == null) {
      System.out.println("index was null for " + dbFilePath.getIndexFilePath() + ". skipping");
      return null;
    }
    if (index.containsKey(key)) {
      ByteOffset byteOffset = index.get(key);
      return GeneralUtils.getValueFromByteOffset(dbFilePath.getDbFilePath(), byteOffset);
    }
    List<String> entries = new ArrayList<>(index.keyset());
    if (key.compareTo(entries.get(0))
        < 0) { // index always has the first db entry. If that entry is greater than key, means the
      // key is not in index
      return null;
    }

    int nearestLeftIndex = GeneralUtils.binarySearch(entries, key) - 1;
    ByteOffset byteOffset = index.get(entries.get(nearestLeftIndex));
    long startByte = byteOffset.getValueStart();
    if (nearestLeftIndex == entries.size() - 1) {
      // search in the last group of the segment
      return GeneralUtils.scanFileForValue(key, dbFilePath.getDbFilePath(), startByte, -1);
    }

    long endByte = index.get(entries.get(nearestLeftIndex + 1)).getValueLengthStart();
    return GeneralUtils.scanFileForValue(key, dbFilePath.getDbFilePath(), startByte, endByte);
  }

  private boolean isElementInserted(String key, String bloomFilterPath) {
    boolean isElementInserted = true;
    if (FeatureFlags.BLOOM_FILTERS_ENABLED) {
      try {
        BloomFilter<String> loadedBloomFilter;
        if (bloomFilterMap.containsKey(bloomFilterPath)) {
          loadedBloomFilter = bloomFilterMap.get(bloomFilterPath);
        } else {
          loadedBloomFilter = BloomFilter.load(bloomFilterPath);
          bloomFilterMap.put(bloomFilterPath, loadedBloomFilter); // caching to reduce disk lookups
        }
        isElementInserted = loadedBloomFilter.contains(key);
      } catch (IOException e) {
        System.out.println("failed to load bloom filter from: " + bloomFilterPath);
      }
    }
    return isElementInserted;
  }

  public void setNewFileAdded(boolean newFileAdded) {
    this.newFileAdded = newFileAdded;
  }
}
