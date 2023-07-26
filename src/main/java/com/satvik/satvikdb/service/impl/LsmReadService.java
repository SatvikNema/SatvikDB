package com.satvik.satvikdb.service.impl;

import com.satvik.satvikdb.context.Index;
import com.satvik.satvikdb.context.Memtable;
import com.satvik.satvikdb.model.ByteOffset;
import com.satvik.satvikdb.model.DbFilePath;
import com.satvik.satvikdb.utils.GeneralUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class LsmReadService {
    private String dbFilePathStartsWith;
    private String indexFilePathStartsWith;

    private String rootDir;

    private List<DbFilePath> diskLookups;

    private boolean newFileAdded;

    public LsmReadService(String rootDir, String dbFilePathStartsWith, String indexFilePathStartsWith) {
        this.dbFilePathStartsWith = dbFilePathStartsWith;
        this.indexFilePathStartsWith = indexFilePathStartsWith;
        this.rootDir = rootDir;
        diskLookups = GeneralUtils.loadFilesSortedByAgeDesc(rootDir, indexFilePathStartsWith, dbFilePathStartsWith);
        newFileAdded = false;
    }

    public String get(String key, Memtable memtable){

        if(memtable.containsKey(key)){
            return memtable.get(key);
        }

        if(newFileAdded){
            diskLookups = GeneralUtils.loadFilesSortedByAgeDesc(rootDir, indexFilePathStartsWith, dbFilePathStartsWith);
            System.out.println("new writes were added before last lookup. refreshing diskLookups entity");
            newFileAdded = false;
        }

        Optional<String> optionalValue = findValueInFiles(key, diskLookups);
        if(optionalValue.isPresent()){
            return optionalValue.get();
        } else{
            System.out.println("Key not present in database :( "+key);
            return null;
        }
    }

    private Optional<String> findValueInFiles(String key, List<DbFilePath> diskLookups) {
        String value;
        Optional<String> result = Optional.empty();
        for(DbFilePath dbFilePath : diskLookups){
            value = findValueInFile(key, dbFilePath);
            if(value!=null){
                result = Optional.of(value);
            }
        }
        return result;
    }

    private String findValueInFile(String key, DbFilePath dbFilePath) {
        String indexPath = dbFilePath.getIndexFilePath();
        Index index = GeneralUtils.loadIndex(indexPath);
        if(index == null) {
            System.out.println("index was null for "+dbFilePath.getIndexFilePath()+". skipping");
            return null;
        }
        if(index.containsKey(key)){
            ByteOffset byteOffset = index.get(key);
            return GeneralUtils.getValueFromByteOffset(dbFilePath.getDbFilePath(), byteOffset);
        }
        List<String> entries = new ArrayList<>(index.keyset());
        if(key.compareTo(entries.get(0)) < 0){ // index always has the first db entry. If that entry is greater than key, means the key is not in index
            return null;
        }

        int nearestLeftIndex = GeneralUtils.binarySearch(entries, key) - 1;
        ByteOffset byteOffset = index.get(entries.get(nearestLeftIndex));
        long startByte = byteOffset.getValueStart();
        if(nearestLeftIndex == entries.size()-1){
            // search in the last group of the segment
            return GeneralUtils.scanFileForValue(key, dbFilePath.getDbFilePath(), startByte, -1);
        }

        long endByte = index.get(entries.get(nearestLeftIndex+1)).getValueLengthStart();
        return GeneralUtils.scanFileForValue(key, dbFilePath.getDbFilePath(), startByte, endByte);

    }

    public void setNewFileAdded(boolean newFileAdded) {
        this.newFileAdded = newFileAdded;
    }
}
