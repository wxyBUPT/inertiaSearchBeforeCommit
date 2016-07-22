package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByGoodId;
import com.alibaba.middleware.race.storage.IndexNameSpace;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 7/19/16.
 */
public class BuildGoodIdThread extends BuildThread<ComparableKeysByGoodId>{

    @Override
    protected void printRawData(ComparableKeysByGoodId comparableKeysByGoodId) {
        System.out.println(fileManager.getRowFromDiskLoc(comparableKeysByGoodId.getDataDiskLoc()));
    }

    public BuildGoodIdThread(AtomicInteger nRemain, CountDownLatch sendFinishSingle){
        super(nRemain,sendFinishSingle);
        this.keysQueue = DiskLocQueues.comparableKeysByGoodIdQueue;
    }

    @Override
    protected void cacheRoot() {
        IndexNameSpace.mGoodRoot = flushUtil.bCacheRoot(IndexNameSpace.goodRoot);
    }

    @Override
    protected void createBPlusTree() {
        IndexNameSpace.goodRoot = flushUtil.buildBPlusTree(sortedKeysInDisk);
    }
}
