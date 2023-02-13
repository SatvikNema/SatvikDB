package com.satvik.satvikdb.service;

import com.satvik.satvikdb.context.Index;

public interface WriteService {
    void write(String key, String value, Index index);
    void shutdown();
}
