package com.satvik.satvikdb;

import com.satvik.satvikdb.service.DbService;
import com.satvik.satvikdb.service.impl.DbFactory;
import com.satvik.satvikdb.utils.TypesEnum;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

class SatvikDbTest {

    @Test
    void testMain() {
        DbService dbService = DbFactory.getDbService(TypesEnum.LSM_SSTABLE);
        Runtime.getRuntime().addShutdownHook(new Thread(dbService::shutdown));

        getSampleMap().forEach(dbService::write);
        getSampleMap().forEach((key, value) -> {
            Assertions.assertEquals("this is a value for "+key, dbService.read(key));
        });
    }

    private static Map<String, String> getSampleMap(){
        Map<String, String> map = new LinkedHashMap<>();
        for(int i=0;i<250;i++){
            int x = i % 100;
            map.put("key"+x, "this is a value for key"+x);
        }
        return map;
    }
}

