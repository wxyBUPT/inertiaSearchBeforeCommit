package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByGoodOrderId;
import com.alibaba.middleware.race.storage.IndexNameSpace;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 7/20/16.
 */
public class BuildGoodOrderIdThread extends BuildThread<ComparableKeysByGoodOrderId>{

    @Override
    protected void printRawData(ComparableKeysByGoodOrderId comparableKeysByGoodOrderId) {
        System.out.println(fileManager.getRowFromDiskLoc(comparableKeysByGoodOrderId.getDataDiskLoc()));
    }

    public BuildGoodOrderIdThread(AtomicInteger nRemain, CountDownLatch sendFinishSingle){
        super(nRemain,sendFinishSingle);
        this.keysQueue = DiskLocQueues.comparableKeysByGoodOrderId;
    }

    @Override
    protected void createBPlusTree() {
        IndexNameSpace.goodOrderRoot = flushUtil.buildBPlusTree(sortedKeysInDisk);
    }
}
