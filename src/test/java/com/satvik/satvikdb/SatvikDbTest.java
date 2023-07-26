package com.satvik.satvikdb;

import com.satvik.satvikdb.service.DbService;
import com.satvik.satvikdb.service.impl.DbFactory;
import com.satvik.satvikdb.utils.TypesEnum;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

class SatvikDbTest {

    private static final int TOTAL_RECORDS = 60000;
    private static final int INTERVAL_1 = 20000;
    private static final int INTERVAL_2 = 40000;

    @Test
    void testMain() {
        DbService dbService = DbFactory.getDbService(TypesEnum.LSM_SSTABLE);
        Runtime.getRuntime().addShutdownHook(new Thread(dbService::shutdown));

        getSampleMap().forEach(dbService::write);
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

// 20000 db entries
// 200 index entries, 1 for each 100
