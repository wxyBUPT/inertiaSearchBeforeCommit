package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.cache.AvlTree;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByOrderId;
import com.alibaba.middleware.race.storage.IndexPartition;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 7/26/16.
 */
public class OrderIdPartionBuildThread extends PartionBuildThread<ComparableKeysByOrderId>{

    public OrderIdPartionBuildThread(AtomicInteger nRemain, CountDownLatch sendFinishSingle){
        super(nRemain,sendFinishSingle);
        this.keysQueue = DiskLocQueues.comparableKeysByOrderId;
        this.myPartions = indexNameSpace.mOrderPartion;
    }

    @Override
    protected void flushAvlToDisk() {
        for(Map.Entry<Integer,AvlTree<ComparableKeysByOrderId>> entry:inMemoryTrees.entrySet()){
            /**
             * 迭代avl,并且将其刷新到disk 中,并将得到的磁盘列表位置存储到对应的partion 中去
             */
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
        for(Map.Entry<Integer,IndexPartition<ComparableKeysByOrderId>> entry:myPartions.entrySet()){
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
