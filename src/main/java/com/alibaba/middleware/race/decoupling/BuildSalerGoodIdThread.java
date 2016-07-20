package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysBySalerIdGoodId;
import com.alibaba.middleware.race.storage.IndexNameSpace;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 7/20/16.
 */
public class BuildSalerGoodIdThread extends BuildThread<ComparableKeysBySalerIdGoodId>{

    @Override
    protected void createBPlusTree() {
        IndexNameSpace.salerGoodRoot = flushUtil.buildBPlusTree(sortedKeysInDisk);
    }

    public BuildSalerGoodIdThread(AtomicInteger nRemain){
        super(nRemain);
        this.keysQueue = DiskLocQueues.comparableKeysBySalerIdGoodId;
    }
}
