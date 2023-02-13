package com.satvik.satvikdb.service.impl;

import com.satvik.satvikdb.service.DbService;
import com.satvik.satvikdb.utils.LsmDbConstants;
import com.satvik.satvikdb.utils.SimpleDbConstants;
import com.satvik.satvikdb.utils.TypesEnum;

public class DbFactory {
    public static DbService getDbService(TypesEnum typesEnum){
        DbService dbService = null;
        switch (typesEnum) {
            case SIMPLE:
                dbService = new SimpleDb();
                dbService.init(SimpleDbConstants.ROOT_DIR, SimpleDbConstants.FILE_NAME, SimpleDbConstants.INDEX_FILE_NAME, null);
            case LSM_SSTABLE:
                dbService = new LsmDb();
                dbService.init(LsmDbConstants.ROOT_DIR, LsmDbConstants.FILE_NAME_STARTS_WITH, LsmDbConstants.INDEX_FILE_NAME_STARTS_WITH, LsmDbConstants.WAL_NAME);
        }
        return dbService;
    }
}
