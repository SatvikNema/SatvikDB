package com.satvik.satvikdb.bloomfilter.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ConversionTest {

  @Test
  @DisplayName("verify conversions between byte and long")
  void testToBytes_1() {
    List<Long> testValues =
        List.of(
            255L,
            (1L << 40) - 1,
            Long.MAX_VALUE,
            0L,
            1L,
            2L,
            4096L,
            4095L,
            4097L,
            Long.MAX_VALUE - 4096);
    for (long i : testValues) {
      byte[] bytes = Conversion.toBytes(i);
      long back = Conversion.toLong(bytes);
      assertEquals(i, back);
    }
  }

  @Test
  @DisplayName("verify conversions between byte and int")
  void testToBytes_2() {
    List<Integer> testValues =
        List.of(
            255,
            (1 << 29) - 1,
            Integer.MAX_VALUE,
            0,
            1,
            2,
            4096,
            4095,
            4097,
            Integer.MAX_VALUE - 4096);
    for (int i : testValues) {
      byte[] bytes = Conversion.toBytes(i);
      long back = Conversion.toInteger(bytes);
      assertEquals(i, back);
    }
  }
}
