package com.satvik.satvikdb.service;

import com.satvik.satvikdb.context.Index;

public interface ReadService {
    String read(String key, Index index);
    void shutdown();
}
