package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByBuyerId;
import com.alibaba.middleware.race.storage.IndexNameSpace;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 7/19/16.
 */
public class BuildBuyerIdThread extends BuildThread<ComparableKeysByBuyerId>{

    @Override
    protected void printRawData(ComparableKeysByBuyerId comparableKeysByBuyerId) {
    }

    @Override
    protected void cacheRoot() {
        IndexNameSpace.mBuyerRoot = flushUtil.bCacheRoot(IndexNameSpace.buyerRoot);
    }

    public BuildBuyerIdThread(AtomicInteger nRemain, CountDownLatch sendFinishSingle){
        super(nRemain,sendFinishSingle);
        this.keysQueue = DiskLocQueues.comparableKeysByBuyerIdQueue;
    }

    @Override
    protected void createBPlusTree() {
        IndexNameSpace.buyerRoot = flushUtil.buildBPlusTree(sortedKeysInDisk);
    }
}
