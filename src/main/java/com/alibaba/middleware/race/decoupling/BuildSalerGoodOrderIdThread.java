package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysBySalerGoodOrderId;
import com.alibaba.middleware.race.storage.IndexNameSpace;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 7/20/16.
 */
public class BuildSalerGoodOrderIdThread extends BuildThread<ComparableKeysBySalerGoodOrderId>{

    public BuildSalerGoodOrderIdThread(AtomicInteger nRemain){
        super(nRemain);
        this.keysQueue = DiskLocQueues.comparableKeysBySalerGoodOrderId;
    }

    @Override
    protected void createBPlusTree() {
        IndexNameSpace.salerGoodOrderRoot = flushUtil.buildBPlusTree(sortedKeysInDisk);
    }

}
