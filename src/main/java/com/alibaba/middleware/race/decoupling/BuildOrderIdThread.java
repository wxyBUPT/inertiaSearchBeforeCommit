package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByOrderId;
import com.alibaba.middleware.race.storage.IndexNameSpace;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 7/19/16.
 */
public class BuildOrderIdThread extends BuildThread<ComparableKeysByOrderId>{

    public BuildOrderIdThread(AtomicInteger nRemain, CountDownLatch sendFinishSingle){
        super(nRemain,sendFinishSingle);
        this.keysQueue = DiskLocQueues.comparableKeysByOrderId;
    }

    @Override
    protected void printRawData(ComparableKeysByOrderId comparableKeysByOrderId) {
        System.out.println(fileManager.getRowFromDiskLoc(comparableKeysByOrderId.getDataDiskLoc()));
    }

    @Override
    protected void cacheRoot() {
        IndexNameSpace.mOrderRoot = flushUtil.bCacheRoot(IndexNameSpace.orderRoot);
    }

    @Override
    protected void createBPlusTree() {
        IndexNameSpace.orderRoot = flushUtil.buildBPlusTree(sortedKeysInDisk);
    }
}
