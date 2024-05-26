package com.satvik.satvikdb.bloomfilter;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.satvik.satvikdb.CommonTestUtil;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BloomFilterTest {

  // parameters are taken from https://hur.st/bloomfilter/

  private static final int ROUND_PLACES = (int) 1e5;

  @Test
  @DisplayName(
      "verify that integer bloom filter works correctly. When elements not present. false positives expected")
  void testCreate_1() {
    BloomFilter<Integer> bloomFilter = BloomFilter.create(1437758, 10);

    int toInsert = 100000;
    for (int i = 0; i < toInsert; i++) {
      bloomFilter.add(i);
    }

    double errors = 0;
    double success = 0;
    for (int i = toInsert; i < toInsert * 3; i++) {
      boolean result = bloomFilter.contains(i);
      if (result) {
        errors++;
      } else {
        success++;
      }
    }
    System.out.println("Success: " + success);
    System.out.println("Errors: " + errors);

    double perc = (errors / success) * 100;
    double errorRate = (double) ((int) (perc * ROUND_PLACES)) / ROUND_PLACES;
    System.out.println("Error rate: " + errorRate);

    Map<Integer, Integer> map = bloomFilter.getSeen();
    Collection<Integer> countArr = map.values();
    Map<Integer, Integer> f = new HashMap<>();
    for (int i : countArr) {
      f.put(i, f.getOrDefault(i, 0) + 1);
    }
    System.out.println(f);

    // fails (with adding bytes and mods):        {1=2049, 2=148415, 3=101409, 4=39000, 5=8030,
    // 6=1097}
    // success (with one time mod on BigInteger): {1=268812, 2=29002, 3=2072, 4=104, 5=10}

  }

  @Test
  @DisplayName(
      "verify that integer bloom filter works correctly. When elements present. No false positives expected")
  void testCreate_2() {
    BloomFilter<Integer> bloomFilter = BloomFilter.create(1437758, 10);

    int toInsert = 100000;
    for (int i = 0; i < toInsert; i++) {
      bloomFilter.add(i);
    }
    for (int i = 0; i < toInsert; i++) {
      assertTrue(bloomFilter.contains(i));
    }

    Map<Integer, Integer> map = bloomFilter.getSeen();
    Collection<Integer> countArr = map.values();
    Map<Integer, Integer> f = new HashMap<>();
    for (int i : countArr) {
      f.put(i, f.getOrDefault(i, 0) + 1);
    }
    System.out.println(f);
  }

  @Test
  @DisplayName(
      "test bloom filter serialization. Disabled by default. To execute, update the filepath")
  @Disabled
  void testCreate_3() throws IOException {
    String filepath =
        "/Users/satvik.nema/Documents/learnings/worlds-simplest-db/lsm-db/bloom_filter_123";
    int size = 200;

    Set<Integer> setBits = CommonTestUtil.getRandomSet();

    BloomFilter<Integer> bloomFilter = BloomFilter.create(size, 10);
    setBits.forEach(bloomFilter::add);

    bloomFilter.serialise(filepath);
    BloomFilter<Integer> loadedBloomFilter = BloomFilter.load(filepath);

    int errorsOg = 0, errorsLoaded = 0;
    int successOg = 0, successLoaded = 0;
    for (int i = 0; i <= size; i++) {
      if (setBits.contains(i)) {
        assertTrue(bloomFilter.contains(i));
        assertTrue(loadedBloomFilter.contains(i));
      } else {
        boolean result = bloomFilter.contains(i);
        if (result) {
          errorsOg++;
        } else {
          successOg++;
        }

        result = loadedBloomFilter.contains(i);
        if (result) {
          errorsLoaded++;
        } else {
          successLoaded++;
        }
      }
    }

    System.out.println("successOg: " + successOg);
    System.out.println("errorsOg: " + errorsOg);
    System.out.println("successLoaded: " + successLoaded);
    System.out.println("errorsLoaded: " + errorsLoaded);
  }
}
