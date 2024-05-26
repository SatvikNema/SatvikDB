package com.satvik.satvikdb.bloomfilter;

import com.satvik.satvikdb.CommonTestUtil;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class BitArrayTest {

    @Test
    @DisplayName("works as expected when size smaller than 64")
    void testSetBit_1() {
        BitArray bitArray = new BitArray(30);
        bitArray.setBit(3);
        assertTrue(bitArray.isBitSet(3));
        assertFalse(bitArray.isBitSet(2));
        assertFalse(bitArray.isBitSet(4));
    }

    @Test
    @DisplayName("works as expected when size greater than 64")
    void testSetBit_2() {
        BitArray bitArray = new BitArray(200);
        bitArray.setBit(3);
        bitArray.setBit(100);

        assertTrue(bitArray.isBitSet(3));
        assertTrue(bitArray.isBitSet(100));
        assertFalse(bitArray.isBitSet(2));
        assertFalse(bitArray.isBitSet(4));
    }

    @Test
    @DisplayName("works as expected when size exactly 64")
    void testSetBit_3() {
        BitArray bitArray = new BitArray(64);
        bitArray.setBit(3);
        bitArray.setBit(64);

        assertTrue(bitArray.isBitSet(3));
        assertTrue(bitArray.isBitSet(64));
        assertFalse(bitArray.isBitSet(2));
        assertFalse(bitArray.isBitSet(4));
    }

    @Test
    @DisplayName("bit boundary validation for 0, 1 and 64")
    void testSetBit_4() {
        BitArray bitArray = new BitArray(100);
        assertFalse(bitArray.isBitSet(0));

        bitArray.setBit(0);
        assertTrue(bitArray.isBitSet(0));
        assertFalse(bitArray.isBitSet(1));
        assertFalse(bitArray.isBitSet(64));

        bitArray.setBit(1);
        assertTrue(bitArray.isBitSet(0));
        assertTrue(bitArray.isBitSet(1));
        assertFalse(bitArray.isBitSet(64));

        bitArray.setBit(64);
        assertTrue(bitArray.isBitSet(0));
        assertTrue(bitArray.isBitSet(1));
        assertTrue(bitArray.isBitSet(64));
    }

    @Test
    @DisplayName("big load test with high number of elements")
    void testSetBit_5() {
        int size = 10000;
        BitArray bitArray = new BitArray(size);

        Set<Integer> setBits = Set.of(0, 1, 8, 16, 32, 64, 128, 4000, 7000, 9999);
        setBits.forEach(bitArray::setBit);

        for(int i=0;i<=size;i++){
            if(setBits.contains(i)){
                assertTrue(bitArray.isBitSet(i));
            } else {
                assertFalse(bitArray.isBitSet(i));
            }
        }
    }

    @Test
    @DisplayName("correctly throws out of bounds exception when size < 64")
    void testSetBit_6() {
        BitArray bitArray = new BitArray(100);
        bitArray.setBit(100);
        bitArray.isBitSet(100);

        assertThrows(IndexOutOfBoundsException.class, () -> bitArray.setBit(101));
        assertThrows(IndexOutOfBoundsException.class, () -> bitArray.isBitSet(101));
    }

    @Test
    @DisplayName("correctly throws out of bounds exception when size > 64")
    void testSetBit_7() {
        BitArray bitArray = new BitArray(30);
        bitArray.setBit(30);
        bitArray.isBitSet(30);

        assertThrows(IndexOutOfBoundsException.class, () -> bitArray.setBit(31));
        assertThrows(IndexOutOfBoundsException.class, () -> bitArray.isBitSet(31));
    }

    @Test
    @DisplayName("works as expected there are negative values set in the bit array (for 63 or 127 or so on)")
    void testSetBit_8() {
        BitArray bitArray = new BitArray(200);

        bitArray.setBit(63);
        assertTrue(bitArray.isBitSet(63));

        bitArray.setBit(127);
        assertTrue(bitArray.isBitSet(127));
    }

    @Test
    @DisplayName("test serialization. Disabled by default. To execute, update the filepath")
    @Disabled
    void testSetBit_9() throws IOException {
        String filepath = "/Users/satvik.nema/Documents/learnings/worlds-simplest-db/lsm-db/bloom_filter_serialise.txt";
        int size = 200;

        Set<Integer> setBits = CommonTestUtil.getRandomSet();
        BitArray bitArray = CommonTestUtil.getPredefinedSetBitArray(setBits);

        bitArray.serialise(filepath);
        BitArray loadedFromFile = BitArray.load(filepath);

        for(int i=0;i<=size;i++){
            if(setBits.contains(i)){
                assertTrue(bitArray.isBitSet(i));
            } else {
                assertFalse(bitArray.isBitSet(i));
            }
        }

        assertEquals(bitArray, loadedFromFile);
        for(int i=0;i<=size;i++){
            if(setBits.contains(i)){
                assertTrue(loadedFromFile.isBitSet(i));
            } else {
                assertFalse(loadedFromFile.isBitSet(i));
            }
        }
    }
}