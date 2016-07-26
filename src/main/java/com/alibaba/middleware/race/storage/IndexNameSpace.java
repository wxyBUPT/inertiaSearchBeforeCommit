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

    public static DiskLoc buyerRoot;
    public static DiskLoc goodRoot;
    public static DiskLoc orderRoot;
    public static DiskLoc buyerCreateTimeOrderRoot;
    public static DiskLoc goodOrderRoot;

    /**
     * 为了每次查询减少一次磁盘访问,将rootNode 取出到内存
     */
    public static IndexNode<ComparableKeysByBuyerId> mBuyerRoot;
    public static IndexNode<ComparableKeysByGoodId> mGoodRoot;


    /**
     * order 所有key 值分片管理
     */
    public static HashMap<Integer,IndexPartition<ComparableKeysByOrderId>> mOrderPartion;
    public static HashMap<Integer,IndexPartition<ComparableKeysByBuyerCreateTimeOrderId>> mBuyerCreateTimeOrderPartion;
    public static HashMap<Integer,IndexPartition<ComparableKeysByGoodOrderId>> mGoodOrderPartions;

    /**
     * 对于buyer 和 good ,在lru 中缓存叶子节点与非叶子节点
     * 只不过非叶子节点的优先级要高于叶子节点(由lru for mindata 负责)
     * orderLRU 由partion 负责
     */
    private final LRUCache<DiskLoc,IndexNode> buyerLRU;
    private final LRUCache<DiskLoc,IndexNode> goodLRU;

    private IndexExtentManager indexExtentManager;
    private StoreExtentManager storeExtentManager;

    private IndexNameSpace(){
        indexExtentManager = IndexExtentManager.getInstance();
        storeExtentManager = StoreExtentManager.getInstance();
        buyerLRU = new ConcurrentLruCacheForMidData<>(RaceConf.N_BUYER_INDEX_CACHE_COUNT);
        goodLRU = new ConcurrentLruCacheForMidData<>(RaceConf.N_GOOD_INDEX_CACHE_COUNT);
        /**
         * 初始化partions
         */
        mOrderPartion = new HashMap<>();
        mBuyerCreateTimeOrderPartion = new HashMap<>();
        mGoodOrderPartions = new HashMap<>();
        for(int i = 0;i<RaceConf.N_PARTITION;i++){
            mOrderPartion.put(i,new IndexPartition<ComparableKeysByOrderId>(i));
            mBuyerCreateTimeOrderPartion.put(i,new IndexPartition<ComparableKeysByBuyerCreateTimeOrderId>(i));
            mGoodOrderPartions.put(i,new IndexPartition<ComparableKeysByGoodOrderId>(i));
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
        IndexNode<ComparableKeysByGoodId> indexNode = mGoodRoot;
        while (!indexNode.isLeafNode()){
            DiskLoc diskLoc = indexNode.search(key);
            if(diskLoc==null)return null;
            /**
             * 从缓存中获得
             */
            IndexNode cacheNode = goodLRU.get(diskLoc);
            indexNode = cacheNode==null?indexExtentManager.getIndexNodeFromDiskLoc(diskLoc):cacheNode;
            if(cacheNode==null)goodLRU.put(diskLoc,indexNode);
        }
        DiskLoc diskLoc = indexNode.search(key);
        if(diskLoc==null)return null;
        return storeExtentManager.getRowFromDiskLoc(diskLoc);
    }

    /**
     * 根据buyerId 查询buyer 数据,之后也需要重构成使用partion
     * @param buyerId
     * @return
     */
    public Row queryBuyerDataByBuyerId(String buyerId){
        ComparableKeysByBuyerId key = new ComparableKeysByBuyerId(buyerId,null);
        IndexNode<ComparableKeysByBuyerId> indexNode = mBuyerRoot;
        while(!indexNode.isLeafNode()){
            DiskLoc diskLoc = indexNode.search(key);
            if(diskLoc==null)return null;

            IndexNode cacheNode = buyerLRU.get(diskLoc);
            indexNode = cacheNode==null?indexExtentManager.getIndexNodeFromDiskLoc(diskLoc):cacheNode;
            if(cacheNode==null)buyerLRU.put(diskLoc,indexNode);
        }
        DiskLoc diskLoc = indexNode.search(key);
        if(diskLoc==null)return null;
        return storeExtentManager.getRowFromDiskLoc(diskLoc);
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
        sb.append("buyerLRU  ###  size : " + buyerLRU.size() + ", limit: "+buyerLRU.getLimit());
        sb.append("goodLRU  ### size: " + goodLRU.size() + ", limit: " + goodLRU.getLimit());
        sb.append(", partion info : " + " null ");
        return sb.toString();
    }

}