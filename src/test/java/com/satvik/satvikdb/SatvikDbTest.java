package com.satvik.satvikdb;

import com.satvik.satvikdb.context.FeatureFlags;
import com.satvik.satvikdb.service.DbService;
import com.satvik.satvikdb.service.impl.DbFactory;
import com.satvik.satvikdb.utils.TypesEnum;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

class SatvikDbTest {

    private static final int TOTAL_RECORDS = 60000;
    private static final int INTERVAL_1 = 20000;
    private static final int INTERVAL_2 = 40000;

    @Test
    @Disabled
    @DisplayName("Do a comprehensive read and write on the database. assert general correctness. No bloom filters involved. Disabled by default. update LsmDbConstants.ROOT_DIR before running")
    void testMain_1() {
        FeatureFlags.BLOOM_FILTERS_ENABLED = false;
        DbService dbService = DbFactory.getDbService(TypesEnum.LSM_SSTABLE);
        Runtime.getRuntime().addShutdownHook(new Thread(dbService::shutdown));

        long start = System.currentTimeMillis();
        writeData(dbService);
        long end = System.currentTimeMillis();
        long duration = end-start;
        System.out.println("time taken to write: "+duration+"ms. without bloom filters");


        start = System.currentTimeMillis();
        readData(dbService);
        end = System.currentTimeMillis();
        duration = end-start;
        System.out.println("time taken to read : "+duration+"ms. without bloom filters");

        /*
        time taken to write: 2155ms. without bloom filters
        time taken to read : 309ms. without bloom filters
         */
    }

    @Test
    @Disabled
    @DisplayName("Do a comprehensive read and write on the database. assert general correctness. With bloom filters. Disabled by default. update LsmDbConstants.ROOT_DIR before running")
    void testMain_2() {
        FeatureFlags.BLOOM_FILTERS_ENABLED = true;
        DbService dbService = DbFactory.getDbService(TypesEnum.LSM_SSTABLE);
        Runtime.getRuntime().addShutdownHook(new Thread(dbService::shutdown));

        long start = System.currentTimeMillis();
        writeData(dbService);
        long end = System.currentTimeMillis();
        long duration = end-start;
        System.out.println("time taken to write: "+duration+"ms. with bloom filters");


        start = System.currentTimeMillis();
        readData(dbService);
        end = System.currentTimeMillis();
        duration = end-start;
        System.out.println("time taken to read : "+duration+"ms. with bloom filters");

        /*
        time taken to write: 2681ms. with bloom filters
        time taken to read : 83ms. with bloom filters
         */

    }

    private void readData(DbService dbService) {
        for(int i=0;i<10;i++){
            String value = dbService.read("key"+i);
            Assertions.assertEquals("this is a overwritten value for key"+i, value);
        }

        for(int i=INTERVAL_1;i<INTERVAL_1+10;i++){
            String value = dbService.read("key"+i);
            Assertions.assertEquals("this is a second overwritten value for key"+i, value);
        }

        for(int i=INTERVAL_2;i<INTERVAL_2+10;i++){
            String value = dbService.read("key"+i);
            Assertions.assertEquals("this is a value for key"+i, value);
        }
    }

    private void writeData(DbService dbService) {
        getSampleMap().forEach(dbService::write);
    }

    private static Map<String, String> getSampleMap(){
        Map<String, String> map = new LinkedHashMap<>();
        for(int i=0;i<TOTAL_RECORDS;i++){
            map.put("key"+i, "this is a value for key"+i);
        }

        for(int i=0;i<INTERVAL_1;i++){
            map.put("key"+i, "this is a overwritten value for key"+i);
        }

        for(int i=INTERVAL_1;i<INTERVAL_2;i++){
            map.put("key"+i, "this is a second overwritten value for key"+i);
        }
        return map;
    }
}
