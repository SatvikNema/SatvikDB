package com.satvik.satvikdb.model;

import java.io.IOException;
import java.io.RandomAccessFile;

public class RafEntry {
  private RandomAccessFile randomAccessFile;

  public RafEntry(RandomAccessFile randomAccessFile, String currentLine) {
    this.randomAccessFile = randomAccessFile;
    this.currentLine = currentLine;
  }

  public RandomAccessFile getRandomAccessFile() {
    return randomAccessFile;
  }

  public void setRandomAccessFile(RandomAccessFile randomAccessFile) {
    this.randomAccessFile = randomAccessFile;
  }

  public void setCurrentLine(String currentLine) {
    this.currentLine = currentLine;
  }

  public String getCurrentLine() {
    return currentLine;
  }

  public String readLine() throws IOException {
    return randomAccessFile.readLine();
  }

  private String currentLine;
}
