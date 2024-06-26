package com.satvik.satvikdb;

import com.satvik.satvikdb.service.DbService;
import com.satvik.satvikdb.service.impl.DbFactory;
import com.satvik.satvikdb.utils.InputThread;
import com.satvik.satvikdb.utils.TypesEnum;
import java.io.IOException;

public class SatvikDb {
  public static void main(String[] args) throws IOException {

    DbService dbService = DbFactory.getDbService(TypesEnum.LSM_SSTABLE);
    Runtime.getRuntime().addShutdownHook(new Thread(dbService::shutdown));

    InputThread ip = new InputThread();
    ip.setDbService(dbService);
    ip.start();
    System.out.println("main has ended!");

    // todo run this on a separate thread in an interval of 5 mins
    //        MergeService service = new MergeService(LsmDbConstants.FILE_NAME_STARTS_WITH,
    // LsmDbConstants.ROOT_DIR, LsmDbConstants.INDEX_FILE_NAME_STARTS_WITH);
    //        service.merge();

  }
}
