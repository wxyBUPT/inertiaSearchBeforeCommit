package com.alibaba.middleware.race.handler;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.decoupling.DiskLocQueues;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByGoodId;
import com.alibaba.middleware.race.storage.DiskLoc;
import com.alibaba.middleware.race.storage.StoreType;

import java.io.IOException;

/**
 * Created by xiyuanbupt on 7/28/16.
 */
public class GoodFileHandler extends DataFileHandler{

    @Override
    void handleLine(String line, DiskLoc diskLoc) throws IOException, OrderSystem.TypeException, InterruptedException {
        diskLoc.setStoreType(StoreType.GOODLINE);
        /**
         * Find goodid and salerid
         */
        int i = line.indexOf("goodid:");
        line = line.substring(i+7);
        i = line.indexOf("\t");
        String goodid = line.substring(0,i);

        if(goodid==null ){
            throw new RuntimeException("Bad data! goodid " + goodid  );
        }
        ComparableKeysByGoodId goodIdKeys = new ComparableKeysByGoodId(goodid,diskLoc);
        DiskLocQueues.comparableKeysByGoodIdQueue.put(goodIdKeys);
    }
}
