package com.satvik.satvikdb.context;

import com.satvik.satvikdb.model.ByteOffset;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import java.util.function.BiConsumer;

public interface Index extends Serializable {
    ByteOffset get(String key);
    ByteOffset put(String key, ByteOffset byteOffset);
    boolean containsKey(String key);

    void printToConsole();

    Set<String> keyset();

    void forEach(BiConsumer<String, ByteOffset> action);
}
