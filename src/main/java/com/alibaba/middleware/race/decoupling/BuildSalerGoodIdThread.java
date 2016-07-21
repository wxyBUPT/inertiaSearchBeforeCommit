package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysBySalerIdGoodId;
import com.alibaba.middleware.race.storage.IndexNameSpace;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 7/20/16.
 */
public class BuildSalerGoodIdThread extends BuildThread<ComparableKeysBySalerIdGoodId>{

    @Override
    protected void createBPlusTree() {
        IndexNameSpace.salerGoodRoot = flushUtil.buildBPlusTree(sortedKeysInDisk);
    }

    @Override
    protected void printRawData(ComparableKeysBySalerIdGoodId comparableKeysBySalerIdGoodId) {
        System.out.println(fileManager.getRowFromDiskLoc(comparableKeysBySalerIdGoodId.getDiskLoc()));
    }

    public BuildSalerGoodIdThread(AtomicInteger nRemain, CountDownLatch sendFinishSingle){
        super(nRemain,sendFinishSingle);
        this.keysQueue = DiskLocQueues.comparableKeysBySalerIdGoodId;
    }
}
