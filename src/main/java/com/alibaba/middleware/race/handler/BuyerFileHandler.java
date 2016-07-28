package com.alibaba.middleware.race.handler;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.decoupling.DiskLocQueues;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByBuyerId;
import com.alibaba.middleware.race.storage.DiskLoc;
import com.alibaba.middleware.race.storage.StoreType;

import java.io.IOException;

/**
 * Created by xiyuanbupt on 7/28/16.
 */
public class BuyerFileHandler extends DataFileHandler{
    @Override
    void handleLine(String line, DiskLoc diskLoc) throws IOException, OrderSystem.TypeException, InterruptedException {
        diskLoc.setStoreType(StoreType.BUYERLINE);

        int i = line.indexOf("buyerid:");
        line = line.substring(i+8);
        i = line.indexOf("\t");
        String buyerid = line.substring(0,i);
        /**
         * Put index info to queue
         */
        ComparableKeysByBuyerId key = new ComparableKeysByBuyerId(buyerid,diskLoc);
        DiskLocQueues.comparableKeysByBuyerIdQueue.put(key);
    }
}
