package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.cache.AvlTree;
import com.alibaba.middleware.race.codec.HashKeyHash;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByGoodOrderId;
import com.alibaba.middleware.race.storage.IndexPartition;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 7/26/16.
 */
public class GoodOrderPartionBuildThread extends PartionBuildThread<ComparableKeysByGoodOrderId>{

    public GoodOrderPartionBuildThread(AtomicInteger nRemain, CountDownLatch sendFinishSingle){
        super(nRemain,sendFinishSingle);
        this.keysQueue = DiskLocQueues.comparableKeysByGoodOrderId;
        this.myPartions = indexNameSpace.mGoodOrderPartions;
    }

    @Override
    protected void putIndexToPartion(ComparableKeysByGoodOrderId comparableKeysByGoodOrderId) {
        int hasCode = HashKeyHash.hashKeyHash(comparableKeysByGoodOrderId.hashCode());
        indexNameSpace.mGoodOrderPartions.get(hasCode).addKey(comparableKeysByGoodOrderId);
    }

    @Override
    protected void createBPlusTree() {
        int size = myPartions.size();
        CountDownLatch countDownLatch = new CountDownLatch(size);
        for(Map.Entry<Integer,IndexPartition<ComparableKeysByGoodOrderId>> entry:myPartions.entrySet()){
            entry.getValue().merageAndBuildMe(countDownLatch);
        }
        /**
         * 等待所有btree 被创建完毕
         */
        try {
            countDownLatch.await(60, TimeUnit.MINUTES);
        }catch (Exception e ){
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
