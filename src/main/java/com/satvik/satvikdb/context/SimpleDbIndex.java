package com.satvik.satvikdb.context;

import com.satvik.satvikdb.model.ByteOffset;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public class SimpleDbIndex implements Index {
    private final Map<String, ByteOffset> index;

    public SimpleDbIndex(Map<String, ByteOffset> index) {
        this.index = index;
    }

    public Map<String, ByteOffset> getIndex(){
        return index;
    }

    @Override
    public ByteOffset get(String key){
        return index.get(key);
    }

    @Override
    public ByteOffset put(String key, ByteOffset byteOffset){
        return index.put(key, byteOffset);
    }

    @Override
    public boolean containsKey(String key){
        return index.containsKey(key);
    }

    @Override
    public synchronized void printToConsole() {
        index.forEach((key, byteOffset) -> {
            System.out.println(key + " "+byteOffset.getValueLengthStart()+" "+byteOffset.getValueStart());
        });
    }

    @Override
    public Set<String> keyset() {
        return index.keySet();
    }

    @Override
    public synchronized void forEach(BiConsumer<String, ByteOffset> action){
        for (Map.Entry<String, ByteOffset> entry : index.entrySet()) {
            String k = entry.getKey();
            ByteOffset v = entry.getValue();
            action.accept(k, v);
        }
    }
}
