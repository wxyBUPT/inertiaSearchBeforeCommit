package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByGoodOrderId;
import com.alibaba.middleware.race.storage.IndexNameSpace;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 7/20/16.
 */
public class BuildGoodOrderIdThread extends BuildThread<ComparableKeysByGoodOrderId>{

    public BuildGoodOrderIdThread(AtomicInteger nRemain){
        super(nRemain);
        this.keysQueue = DiskLocQueues.comparableKeysByGoodOrderId;
    }

    @Override
    protected void createBPlusTree() {
        IndexNameSpace.goodOrderRoot = flushUtil.buildBPlusTree(sortedKeysInDisk);
    }
}
