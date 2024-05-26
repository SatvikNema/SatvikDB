package com.satvik.satvikdb.bloomfilter.utils;

public class Conversion {
  private Conversion() {}

  public static byte[] toBytes(Long val) {
    int bytesInALong = Long.BYTES;
    byte[] result = new byte[bytesInALong];

    int mask = (1 << (Byte.SIZE + 1)) - 1; // 8 bit mask with -> 11111111
    for (int i = bytesInALong - 1; i >= 0; i--) {
      byte end8Bits = (byte) (val & mask);
      result[i] = end8Bits;
      val = val >> Byte.SIZE;
    }
    return result;
  }

  public static Long toLong(byte[] arr) {
    int bytesInALong = Long.BYTES;
    if (arr.length > bytesInALong) {
      throw new ArithmeticException(
          "Cannot convert bytes to long as byte array has more elements than 1 long can handle");
    }
    long result = 0L;
    for (int i = 0; i < bytesInALong; i++) {
      result = result << Byte.SIZE;

      int b = arr[i] & 0xFF;
      result = result | b;
    }
    return result;
  }

  public static byte[] toBytes(Integer val) {
    int bytesInAInt = Integer.BYTES;
    byte[] result = new byte[bytesInAInt];

    int mask = (1 << (Byte.SIZE + 1)) - 1;
    for (int i = bytesInAInt - 1; i >= 0; i--) {
      byte end8Bits = (byte) (val & mask);
      result[i] = end8Bits;
      val = val >> Byte.SIZE;
    }
    return result;
  }

  public static Integer toInteger(byte[] arr) {
    int bytesInAInt = Integer.BYTES;
    if (arr.length > bytesInAInt) {
      throw new ArithmeticException(
          "Cannot convert bytes to int as byte array has more elements than 1 int can handle");
    }
    int result = 0;
    for (int i = 0; i < bytesInAInt; i++) {
      result = result << Byte.SIZE;

      int b = arr[i] & 0xFF;
      result = result | b;
    }
    return result;
  }
}
