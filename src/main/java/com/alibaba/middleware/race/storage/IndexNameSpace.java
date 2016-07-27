package com.alibaba.middleware.race.storage;

import com.alibaba.middleware.race.RaceConf;
import com.alibaba.middleware.race.cache.ConcurrentLruCache;
import com.alibaba.middleware.race.cache.ConcurrentLruCacheForBigData;
import com.alibaba.middleware.race.cache.ConcurrentLruCacheForMidData;
import com.alibaba.middleware.race.cache.LRUCache;
import com.alibaba.middleware.race.codec.HashKeyHash;
import com.alibaba.middleware.race.decoupling.PartionBuildThread;
import com.alibaba.middleware.race.models.Row;
import com.alibaba.middleware.race.models.comparableKeys.*;

import java.io.Serializable;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Logger;

/**
 * Created by xiyuanbupt on 7/20/16.
 * 存储索引元数据
 * 对 index node 的缓存放在这里
 */
public class IndexNameSpace {

    private static Logger LOG = Logger.getLogger(IndexNameSpace.class.getName());

    /**
     * 单例
     */
    protected static IndexNameSpace indexNameSpace;

    public static synchronized IndexNameSpace getInstance(){
        if(indexNameSpace==null){
            indexNameSpace = new IndexNameSpace();
        }
        return indexNameSpace;
    }


    /**
     * order 所有key 值分片管理
     */
    public static HashMap<Integer,IndexPartition<ComparableKeysByOrderId>> mOrderPartion;
    public static HashMap<Integer,IndexPartition<ComparableKeysByBuyerCreateTimeOrderId>> mBuyerCreateTimeOrderPartion;
    public static HashMap<Integer,IndexPartition<ComparableKeysByGoodOrderId>> mGoodOrderPartions;

    /**
     * 加入buyer 和 good
     */
    public static HashMap<Integer,IndexPartition<ComparableKeysByBuyerId>> mBuyer;
    public static HashMap<Integer,IndexPartition<ComparableKeysByGoodId>> mGood;


    private IndexNameSpace(){
        /**
         * 初始化partions
         */
        mOrderPartion = new HashMap<>();
        mBuyerCreateTimeOrderPartion = new HashMap<>();
        mGoodOrderPartions = new HashMap<>();
        mBuyer = new HashMap<>();
        mGood = new HashMap<>();
        for(int i = 0;i<RaceConf.N_PARTITION;i++){
            mOrderPartion.put(i,new IndexPartition<ComparableKeysByOrderId>(i));
            mBuyerCreateTimeOrderPartion.put(i,new IndexPartition<ComparableKeysByBuyerCreateTimeOrderId>(i));
            mGoodOrderPartions.put(i,new IndexPartition<ComparableKeysByGoodOrderId>(i));
            mBuyer.put(i,new IndexPartition<ComparableKeysByBuyerId>(i));
            mGood.put(i,new IndexPartition<ComparableKeysByGoodId>(i));
        }
    }

    public Row queryOrderDataByOrderId(Long orderId){

        ComparableKeysByOrderId key = new ComparableKeysByOrderId(orderId,null);
        /**
         * 确定在哪个partions
         */
        Integer hashCode = HashKeyHash.hashKeyHash(key.hashCode());
        return mOrderPartion.get(hashCode).queryByKey(key);
    }

    /**
     * 根据goodid 查询,之后也需要重构成使用partion
     * @param goodId
     * @return
     */
    public Row queryGoodDataByGoodId(String goodId){
        ComparableKeysByGoodId key = new ComparableKeysByGoodId(goodId,null);
        /**
         * 去顶在哪个partions
         */
        Integer hashCode = HashKeyHash.hashKeyHash(key.hashCode());
        return mGood.get(hashCode).queryByKey(key);
    }

    /**
     * 根据buyerId 查询buyer 数据,之后也需要重构成使用partion
     * @param buyerId
     * @return
     */
    public Row queryBuyerDataByBuyerId(String buyerId){
        ComparableKeysByBuyerId key = new ComparableKeysByBuyerId(buyerId,null);
        /**
         * 确定在哪个partions
         */
        Integer hashCode = HashKeyHash.hashKeyHash(key.hashCode());
        return mBuyer.get(hashCode).queryByKey(key);
    }

    /**
     *
     * @param startTime
     * @param endTime
     * @param buyerid
     * @return
     */
    public Deque<Row> queryOrderDataByBuyerCreateTime(long startTime,long endTime,String buyerid){
        ComparableKeysByBuyerCreateTimeOrderId startKey = new ComparableKeysByBuyerCreateTimeOrderId(
                buyerid,startTime,Long.MIN_VALUE,null
        );
        ComparableKeysByBuyerCreateTimeOrderId endKey = new ComparableKeysByBuyerCreateTimeOrderId(
                buyerid,endTime-1,Long.MAX_VALUE,null
        );
        Integer startHashCode = HashKeyHash.hashKeyHash(startKey.hashCode());
        Integer endHashCode = HashKeyHash.hashKeyHash(endKey.hashCode());
        if(!startHashCode.equals(endHashCode)){
            LOG.info("Some bug happen,this two value should hava same hash code, startKey is: " +
            startKey + ", endKey is : " + endKey);
            System.exit(-1);
        }
        /**
         * 获得数据操作交给对应的partion
         */
        return mBuyerCreateTimeOrderPartion.get(startHashCode).rangeQuery(startKey,endKey);
    }

    public Queue<Row> queryOrderDataByGoodid(String goodid){
        ComparableKeysByGoodOrderId minKey = new ComparableKeysByGoodOrderId(goodid,Long.MIN_VALUE);
        ComparableKeysByGoodOrderId maxKey = new ComparableKeysByGoodOrderId(goodid,Long.MAX_VALUE);
        Integer minHash = HashKeyHash.hashKeyHash(minKey.hashCode());
        Integer maxHash = HashKeyHash.hashKeyHash(maxKey.hashCode());
        if(!minHash.equals(maxHash)){
            LOG.info("Some bug happen,this two value should have same hash code, minKey is : "
            + minHash + ", maxKey is : " + maxHash);
            System.exit(-1);
        }
        /**
         * 获得数据的操作同样交给对应的partion
         */
        return mGoodOrderPartions.get(minHash).rangeQuery(minKey,maxKey);
    }

    /**
     * 获得Namespace 的状态,用于打日志
     * @return
     */
    public String getInfo(){
        StringBuilder sb = new StringBuilder();
        sb.append(", partion info : " + " now is ");
        return sb.toString();
    }

}