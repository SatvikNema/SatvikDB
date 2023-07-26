package com.satvik.satvikdb.service.impl;

import com.satvik.satvikdb.model.DbFilePath;
import com.satvik.satvikdb.model.KeyValuePair;
import com.satvik.satvikdb.model.RafEntry;
import com.satvik.satvikdb.utils.GeneralUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


public class MergeService {
    private String dbFileStartsWith;
    private String rootDir;

    private String indexFileStartsWith;

    public MergeService(String dbFileStartsWith, String rootDir, String indexFileStartsWith) {
        this.dbFileStartsWith = dbFileStartsWith;
        this.rootDir = rootDir;
        this.indexFileStartsWith = indexFileStartsWith;
    }

    public void merge() throws IOException {
        List<DbFilePath> files = GeneralUtils.loadFilesSortedByAgeDesc(rootDir, indexFileStartsWith, dbFileStartsWith);
        Collections.reverse(files);
        int batch = 10;

        List<DbFilePath> filesToDeleteAfterMerge = List.copyOf(files);

        while(!files.isEmpty()){
            int indexTillPartition = batch >= files.size() ? files.size() : batch;
            List<DbFilePath> filesToMerge = files.subList(0, indexTillPartition);
            if(filesToMerge.size() == 1){
                break;
            }
            mergeFiles(filesToMerge);

            files.removeAll(filesToMerge);
            System.out.println("====  PARTITION COMPLETED  ====");
        }
        GeneralUtils.deleteFiles(filesToDeleteAfterMerge);
        // will survive: java_database_1676113178809
    }

    private void mergeFiles(List<DbFilePath> filesToMerge) throws IOException {
        List<String> dbFiles = filesToMerge
                .stream()
                .map(DbFilePath::getDbFilePath)
                .collect(Collectors.toList());

        List<RandomAccessFile> rafs = new ArrayList<>();
        for(String path:dbFiles){
            rafs.add(new RandomAccessFile(path, "r"));
        }
        mergeFilesHelper(rafs);

    }

    private void mergeFilesHelper(List<RandomAccessFile> rafs) throws IOException {
        String minKey;
        Set<String> keys = new HashSet<>();
        List<RafEntry> entries = new ArrayList<>();
        for(RandomAccessFile raf : rafs){
            entries.add(new RafEntry(raf, raf.readLine()));
        }
        RafEntry entryWithLeastKey = null;

        List<KeyValuePair> dataToWrite = new ArrayList<>();

        int mergedContentSize = 18000; // batch size -> number of keys the new files should have

        while(true){
            minKey = "~";
            entryWithLeastKey = null;
            for(RafEntry entry : entries){
                if(entry.getCurrentLine() == null) continue;
                String key = GeneralUtils.getParsedKey(entry.getCurrentLine().toCharArray());
                while(keys.contains(key)){
                    entry.setCurrentLine(entry.readLine());
                    if(entry.getCurrentLine() == null) {
                        key = "~";
                        break;
                    } else {
                        key = GeneralUtils.getParsedKey(entry.getCurrentLine().toCharArray());
                    }
                }
                if(key !=null && minKey.compareTo(key) >= 0){ // if key is same, take this one, as the current file is latest
                    minKey = key;
                    entryWithLeastKey = entry;
                }
            }
            if("~".equals(minKey)){
                break;
            }
            keys.add(minKey);

            if(dataToWrite.size() >= mergedContentSize){
                writeToNewDbFile(dataToWrite);
                dataToWrite = new ArrayList<>();
            }

            dataToWrite.add(new KeyValuePair(minKey, GeneralUtils.getParsedValue(entryWithLeastKey.getCurrentLine().toCharArray())));
            entryWithLeastKey.setCurrentLine(entryWithLeastKey.readLine());
        }
        if(!dataToWrite.isEmpty()){
            writeToNewDbFile(dataToWrite);
        }
        System.out.println("merge completed!");
    }

    private void writeToNewDbFile(List<KeyValuePair> data){
        DbFilePath dbFilePath = LsmDb.createNewFiles(rootDir, indexFileStartsWith, dbFileStartsWith);
        long startSize = 0;
        GeneralUtils.writeToDisk(dbFilePath, startSize, data);
    }

}
