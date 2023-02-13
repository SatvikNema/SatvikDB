package com.satvik.satvikdb.context;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

public class Memtable {
    private SortedMap<String, String> treeMap;
    private AtomicLong mapSize;
    private final long threshold;

    public Memtable(long threshold) {
        this.treeMap = new TreeMap<>();
        this.threshold = threshold;
        mapSize = new AtomicLong(0);
    }

    public boolean isThresholdPassed(){
        return mapSize.get() > threshold;
    }

    public synchronized String put(String key, String value){
        if(!containsKey(key)) {
            int length = key.length() + value.length();
            mapSize.getAndAdd(length);
        }
        return treeMap.put(key, value);
    }

    public String get(String key){
        return treeMap.get(key);
    }

    public long getThreshold() {
        return threshold;
    }

    public synchronized boolean containsKey(String key){
        return treeMap.containsKey(key);
    }

    public List<String> keySet(){
        return new ArrayList<>(treeMap.keySet());
    }

    public synchronized void forEach(BiConsumer<String, String> action){
        for (Map.Entry<String, String> entry : treeMap.entrySet()) {
            String k = entry.getKey();
            String v = entry.getValue();
            action.accept(k, v);
        }
    }

    public long getSize(){
        return mapSize.get();
    }

}
