package com.satvik.satvikdb.model;

/**
 * represents an entry in the db file
 * KeyValuePair is the data
 * byteOffset contains the location at which the key value pair is present
 */
public class FileEntry {
    private KeyValuePair keyValuePair;
    private ByteOffset byteOffset;

    public FileEntry(){}

    public FileEntry(KeyValuePair keyValuePair, ByteOffset byteOffset) {
        this.keyValuePair = keyValuePair;
        this.byteOffset = byteOffset;
    }

    public KeyValuePair getKeyValuePair() {
        return keyValuePair;
    }

    public void setKeyValuePair(KeyValuePair keyValuePair) {
        this.keyValuePair = keyValuePair;
    }

    public ByteOffset getByteOffset() {
        return byteOffset;
    }

    public void setByteOffset(ByteOffset byteOffset) {
        this.byteOffset = byteOffset;
    }

    public static FileEntry getDefaultBuilder(){
        return new FileEntry();
    }

    public FileEntry byteOffset(ByteOffset byteOffset){
        this.setByteOffset(byteOffset);
        return this;
    }

    public FileEntry keyValuePair(KeyValuePair keyValuePair){
        this.setKeyValuePair(keyValuePair);
        return this;
    }

}
