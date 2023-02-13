package com.satvik.satvikdb.model;

public class DbFilePath {
    private String dbFilePath;
    private String indexFilePath;

    public DbFilePath() {
    }

    public DbFilePath(String dbFilePath, String indexFilePath) {
        this.dbFilePath = dbFilePath;
        this.indexFilePath = indexFilePath;
    }

    public String getDbFilePath() {
        return dbFilePath;
    }

    public void setDbFilePath(String dbFilePath) {
        this.dbFilePath = dbFilePath;
    }

    public String getIndexFilePath() {
        return indexFilePath;
    }

    public void setIndexFilePath(String indexFilePath) {
        this.indexFilePath = indexFilePath;
    }
}
