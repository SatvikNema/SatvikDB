package com.satvik.satvikdb.bloomfilter;

import com.satvik.satvikdb.bloomfilter.utils.Conversion;
import com.satvik.satvikdb.bloomfilter.utils.HashAlgorithm;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BloomFilter<T> {

  private final BitArray bitArray;

  private final int size;

  private final int hashAlgorithmsNumber;
  private List<MessageDigest> messageDigests;
  private Map<T, Integer> seenCount =
      new HashMap<>(); // this will just keep track of how many same hash values we've seen so far

  // and how many times while writing to bloom filter. No functional purpose

  public static <E> BloomFilter<E> create(int size, int hashAlgorithmsNumber) {
    return new BloomFilter<>(size, hashAlgorithmsNumber, new BitArray(size));
  }

  public static <E> BloomFilter<E> create(int size, int hashAlgorithmsNumber, BitArray bitArray) {
    return new BloomFilter<>(size, hashAlgorithmsNumber, bitArray);
  }

  public static <E> BloomFilter<E> create(int numberOfInsertions, double errorThreshold) {
    int b = getOptimalBitArraySize(numberOfInsertions, errorThreshold);
    int k = getOptimalNumberOfHashFunctions(numberOfInsertions, b);
    return new BloomFilter<>(b, k, new BitArray(b));
  }

  private BloomFilter(int size, int hashAlgorithmsNumber, BitArray bitArray) {
    this.size = size;
    this.hashAlgorithmsNumber = hashAlgorithmsNumber;
    this.bitArray = bitArray;
    this.messageDigests = new ArrayList<>();

    int added = 0;
    for (HashAlgorithm hashAlgorithm : HashAlgorithm.values()) {
      try {
        messageDigests.add(MessageDigest.getInstance(hashAlgorithm.toString()));
      } catch (NoSuchAlgorithmException e) {
        System.out.println(
            "Hash function "
                + hashAlgorithm
                + " appears to be missing from jdk. Trying out the next available algorithm");
        continue;
      }
      added++;
      if (added == hashAlgorithmsNumber) {
        break;
      }
    }
  }

  public void add(T element) {
    for (MessageDigest md : messageDigests) {
      int bitToSet = getBitPositionAfterHash(element, md);
      bitArray.setBit(bitToSet);
    }
  }

  private int getBitPositionAfterHash(T element, MessageDigest messageDigest) {
    byte[] hashValue = messageDigest.digest(String.valueOf(element).getBytes());

    int bitToSet = getIntegerValueFromHash(hashValue);
    messageDigest.reset();
    seenCount.put(element, seenCount.getOrDefault(bitToSet, 0) + 1);
    return bitToSet;
  }

  private int getIntegerValueFromHash(byte[] hashValue) {
    int bitToSet = 0;
    // this is a bad idea. This occurs in terrible collisions and repeated counts in seenCount
    // The BigInteger way, tho expensive, is the way to go.
    //        for(byte b : hashValue){
    //            bitToSet = (bitToSet + Math.abs(b)) % size;
    //        }
    bitToSet = Math.abs(new BigInteger(1, hashValue).intValue()) % size;
    return bitToSet;
  }

  public boolean contains(T element) {
    boolean result = true;
    for (MessageDigest md : messageDigests) {
      int bitPosition = getBitPositionAfterHash(element, md);
      if (!bitArray.isBitSet(bitPosition)) {
        result = false;
        break;
      }
    }
    return result;
  }

  public Map<T, Integer> getSeen() {
    return seenCount;
  }

  public void serialise(String filepath) throws IOException {
    String bitArrayPath = filepath + "_bit_array";

    bitArray.serialise(bitArrayPath);

    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filepath))) {
      bos.write(Conversion.toBytes(size));
      bos.write(Conversion.toBytes(hashAlgorithmsNumber));
    }
  }

  public static <E> BloomFilter<E> load(String filepath) throws IOException {
    String bitArrayPath = filepath + "_bit_array";

    BitArray loadedBitArray = BitArray.load(bitArrayPath);

    int size;
    int numberOfHashes;
    try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(filepath))) {
      byte[] bytes = bis.readNBytes(Integer.BYTES);
      size = Conversion.toInteger(bytes);

      bytes = bis.readNBytes(Integer.BYTES);
      numberOfHashes = Conversion.toInteger(bytes);
    }
    return create(size, numberOfHashes, loadedBitArray);
  }

  public BitArray getBitArray() {
    return bitArray.copy();
  }

  private static int getOptimalBitArraySize(long n, double E) {
    return (int) (-n * Math.log(E) / (Math.log(2) * Math.log(2)));
  }

  static int getOptimalNumberOfHashFunctions(long n, long b) {
    return Math.max(1, (int) Math.round((double) b / n * Math.log(2)));
  }

  public int getK() {
    return hashAlgorithmsNumber;
  }

  public int getB() {
    return size;
  }
}
