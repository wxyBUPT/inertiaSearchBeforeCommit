package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.cache.AvlTree;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByBuyerCreateTimeOrderId;
import com.alibaba.middleware.race.storage.IndexPartition;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 7/26/16.
 */
public class BuyerTimeOrderPartionBuildThread extends  PartionBuildThread<ComparableKeysByBuyerCreateTimeOrderId>{

    public BuyerTimeOrderPartionBuildThread(AtomicInteger nRemain, CountDownLatch sendFinishSingle){
        super(nRemain,sendFinishSingle);
        this.keysQueue = DiskLocQueues.comparableKeysByBuyerCreateTimeOrderId;
        this.myPartions = indexNameSpace.mBuyerCreateTimeOrderPartion;
    }
    @Override
    protected void flushAvlToDisk() {
        for(Map.Entry<Integer,AvlTree<ComparableKeysByBuyerCreateTimeOrderId>> entry:inMemoryTrees.entrySet()){
            myPartions.get(entry.getKey()).addSortedKeys(
                    flushUtil.moveIteratorDataToDisk(entry.getValue().iterator())
            );
            entry.getValue().makeEmpty();
        }
    }

    @Override
    protected void createBPlusTree() {
        int size = myPartions.size();
        CountDownLatch countDownLatch = new CountDownLatch(size);
        for(Map.Entry<Integer,IndexPartition<ComparableKeysByBuyerCreateTimeOrderId>> entry:myPartions.entrySet()){
            entry.getValue().merageAndBuildMe(countDownLatch);
        }
        /**
         * 等待所有索引被创建完毕
         */
        try {
            countDownLatch.await(60, TimeUnit.MINUTES);
        }catch (Exception e){
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
