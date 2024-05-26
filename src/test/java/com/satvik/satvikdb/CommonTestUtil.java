package com.satvik.satvikdb;

import com.satvik.satvikdb.bloomfilter.BitArray;

import java.util.Set;

public class CommonTestUtil {
    private CommonTestUtil(){}

    public static BitArray getPredefinedSetBitArray(Set<Integer> setBits){
        BitArray bitArray = new BitArray(200);
        setBits.forEach(bitArray::setBit);
        return bitArray;
    }

    public static Set<Integer> getRandomSet(){
        return Set.of(0, 1, 7, 8, 16, 20, 25, 32, 64, 73, 128, 158, 199);
    }
}
