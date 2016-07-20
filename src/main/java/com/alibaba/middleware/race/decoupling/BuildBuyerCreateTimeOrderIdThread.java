package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.RaceConf;
import com.alibaba.middleware.race.cache.LimitedAvlTree;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByBuyerCreateTimeOrderId;
import com.alibaba.middleware.race.storage.DiskLoc;
import com.alibaba.middleware.race.storage.IndexNameSpace;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Created by xiyuanbupt on 7/20/16.
 */
public class BuildBuyerCreateTimeOrderIdThread extends BuildThread<ComparableKeysByBuyerCreateTimeOrderId>{

    public BuildBuyerCreateTimeOrderIdThread(AtomicInteger nRemain){
        super(nRemain);
        this.keysQueue = DiskLocQueues.comparableKeysByBuyerCreateTimeOrderId;
    }

    @Override
    protected void createBPlusTree() {
        IndexNameSpace.buyerCreateTimeOrderRoot = flushUtil.buildBPlusTree(sortedKeysInDisk);
    }
}
