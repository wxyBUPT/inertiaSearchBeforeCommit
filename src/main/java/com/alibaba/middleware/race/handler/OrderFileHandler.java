package com.alibaba.middleware.race.handler;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.decoupling.DiskLocQueues;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByBuyerCreateTimeOrderId;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByGoodOrderId;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByOrderId;
import com.alibaba.middleware.race.storage.DiskLoc;
import com.alibaba.middleware.race.storage.StoreType;

import java.io.IOException;

/**
 * Created by xiyuanbupt on 7/28/16.
 */
public class OrderFileHandler extends DataFileHandler{
    @Override
    void handleLine(String line, DiskLoc diskLoc) throws IOException, OrderSystem.TypeException, InterruptedException {
        diskLoc.setStoreType(StoreType.ORDERLINE);
        int i = line.indexOf("orderid:");
        String tmp = line.substring(i+8);
        i = tmp.indexOf("\t");
        String orderidStr = tmp.substring(0,i);
        Long orderid= Long.parseLong(orderidStr);

        i = line.indexOf("goodid:");
        tmp = line.substring(i+7);
        i = tmp.indexOf("\t");
        String goodid = tmp.substring(0,i);

        i = line.indexOf("buyerid:");
        tmp = line.substring(i+8);
        i = tmp.indexOf("\t");
        String buyerid = tmp.substring(0,i);

        i = line.indexOf("createtime:");
        tmp = line.substring(i+11);
        i = tmp.indexOf("\t");
        String createtimeStr = tmp.substring(0,i);
        Long createtime = Long.parseLong(createtimeStr);

        ComparableKeysByOrderId orderIdKeys = new ComparableKeysByOrderId(orderid,diskLoc);
        DiskLocQueues.comparableKeysByOrderId.put(orderIdKeys);

        ComparableKeysByBuyerCreateTimeOrderId buyerCreateTimeOrderId = new ComparableKeysByBuyerCreateTimeOrderId(
                buyerid, createtime, orderid,diskLoc
        );
        DiskLocQueues.comparableKeysByBuyerCreateTimeOrderId.put(buyerCreateTimeOrderId);

        ComparableKeysByGoodOrderId goodOrderKeys = new ComparableKeysByGoodOrderId(
                goodid,orderid,diskLoc
        );
        DiskLocQueues.comparableKeysByGoodOrderId.put(goodOrderKeys);
    }
}
