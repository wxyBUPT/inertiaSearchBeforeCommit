package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.cache.AvlTree;
import com.alibaba.middleware.race.codec.HashKeyHash;
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
    protected void putIndexToPartion(ComparableKeysByBuyerCreateTimeOrderId comparableKeysByBuyerCreateTimeOrderId) {
        int hashCode = HashKeyHash.hashKeyHash(comparableKeysByBuyerCreateTimeOrderId.hashCode());
        indexNameSpace.mBuyerCreateTimeOrderPartion.get(hashCode).addKey(comparableKeysByBuyerCreateTimeOrderId);
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
