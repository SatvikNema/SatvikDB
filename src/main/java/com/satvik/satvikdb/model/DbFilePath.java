package com.satvik.satvikdb.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DbFilePath {
    private String dbFilePath;
    private String indexFilePath;
    private String bloomFilterPath;

    public DbFilePath(String dbFilePath, String indexFilePath) {
        this.dbFilePath = dbFilePath;
        this.indexFilePath = indexFilePath;
    }

    public DbFilePath(String dbFilePath, String indexFilePath, String bloomFilterPath) {
        this.dbFilePath = dbFilePath;
        this.indexFilePath = indexFilePath;
        this.bloomFilterPath = bloomFilterPath;
    }
}
