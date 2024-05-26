package com.satvik.satvikdb.bloomfilter;

import com.satvik.satvikdb.bloomfilter.utils.Conversion;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class BitArray {

  private final long[] arr;
  private final int size;

  public BitArray(int size) {
    int buckets = (size / 64) + 1; // or (size >> 6) + 1
    arr = new long[buckets];
    this.size = size + 1;
  }

  public BitArray(long[] arr) {
    this.arr = arr;
    this.size = arr.length * 64;
  }

  public void setBit(int position) {
    validateQuery(position, size);

    if (isBitSet(position)) return;

    int bucket = (position / 64);
    int offset = position % 64;
    long valueToBitWiseAnd = 1L << offset;

    arr[bucket] = arr[bucket] | valueToBitWiseAnd;
  }

  public boolean isBitSet(int position) {
    validateQuery(position, size);
    int bucket = (position / 64);
    int offset = position % 64;
    long valueToBitWiseAnd = 1L << offset;

    return (arr[bucket] & valueToBitWiseAnd) != 0;
  }

  private void validateQuery(int position, int limit) {
    if (position >= limit || position < 0) {
      throw new IndexOutOfBoundsException();
    }
  }

  public void serialise(String filepath) throws IOException {
    List<byte[]> bytesToWrite = getSerialisedArray();
    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filepath))) {
      for (byte[] bytes : bytesToWrite) {
        bos.write(bytes);
      }
      bos.flush();
    }
  }

  private List<byte[]> getSerialisedArray() {
    List<byte[]> result = new ArrayList<>();
    for (long i : arr) {
      byte[] bytes = Conversion.toBytes(i);
      result.add(bytes);
    }
    return result;
  }

  public static BitArray load(String filepath) throws IOException {
    List<Long> arrLong = new ArrayList<>();
    try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(filepath))) {
      byte[] bytes = bis.readNBytes(Long.BYTES);
      while (bytes.length > 0) {
        long val = Conversion.toLong(bytes);
        arrLong.add(val);
        bytes = bis.readNBytes(Long.BYTES);
      }
    }
    int size = arrLong.size();
    long[] arr = new long[size];
    for (int i = 0; i < arrLong.size(); i++) {
      arr[i] = arrLong.get(i);
    }
    return new BitArray(arr);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BitArray bitArray = (BitArray) o;
    return Objects.deepEquals(arr, bitArray.arr);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(arr);
  }

  public BitArray copy() {
    long[] copyArr = Arrays.copyOfRange(arr, 0, arr.length);
    return new BitArray(copyArr);
  }
}
