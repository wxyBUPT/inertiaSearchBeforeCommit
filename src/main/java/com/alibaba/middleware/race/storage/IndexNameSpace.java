package com.alibaba.middleware.race.storage;

import com.alibaba.middleware.race.models.Row;
import com.alibaba.middleware.race.models.comparableKeys.*;

import java.io.Serializable;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by xiyuanbupt on 7/20/16.
 * 存储索引元数据
 */
public class IndexNameSpace {
    public static DiskLoc buyerRoot;
    public static DiskLoc goodRoot;
    public static DiskLoc orderRoot;
    public static DiskLoc buyerCreateTimeOrderRoot;
    public static DiskLoc goodOrderRoot;
    public static DiskLoc salerGoodRoot;

    /**
     * 为了每次查询减少一次磁盘访问,将rootNode 取出到内存
     */
    public static IndexNode<ComparableKeysByBuyerId> mBuyerRoot;
    public static IndexNode<ComparableKeysByGoodId> mGoodRoot;
    public static IndexNode<ComparableKeysByOrderId> mOrderRoot;
    public static IndexNode<ComparableKeysByBuyerCreateTimeOrderId> mBuyerCreateTimeOrderRoot;
    public static IndexNode<ComparableKeysByGoodOrderId> mGoodOrderRoot;
    public static IndexNode<ComparableKeysBySalerIdGoodId> mSalerGoodRoot;

    private IndexExtentManager indexExtentManager;
    private StoreExtentManager storeExtentManager;

    public IndexNameSpace(){
        indexExtentManager = IndexExtentManager.getInstance();
        storeExtentManager = StoreExtentManager.getInstance();
    }

    public Row queryOrderDataByOrderId(Long orderId){
        ComparableKeysByOrderId key = new ComparableKeysByOrderId(orderId,null);
        IndexNode<ComparableKeysByOrderId> indexNode = mOrderRoot;
        while(!indexNode.isLeafNode()){
            DiskLoc diskLoc = indexNode.search(key);
            if(diskLoc == null)return null;
            indexNode = indexExtentManager.getIndexNodeFromDiskLoc(diskLoc);
        }
        DiskLoc diskLoc = indexNode.search(key);
        if(diskLoc==null)return null;
        return storeExtentManager.getRowFromDiskLoc(diskLoc);
    }

    public Row queryGoodDataByGoodId(String goodId){
        ComparableKeysByGoodId key = new ComparableKeysByGoodId(goodId,null);
        IndexNode<ComparableKeysByGoodId> indexNode = mGoodRoot;
        while (!indexNode.isLeafNode()){
            DiskLoc diskLoc = indexNode.search(key);
            if(diskLoc==null)return null;
            indexNode = indexExtentManager.getIndexNodeFromDiskLoc(diskLoc);
        }
        DiskLoc diskLoc = indexNode.search(key);
        if(diskLoc==null)return null;
        return storeExtentManager.getRowFromDiskLoc(diskLoc);
    }

    public Row queryBuyerDataByBuyerId(String buyerId){
        ComparableKeysByBuyerId key = new ComparableKeysByBuyerId(buyerId,null);
        IndexNode<ComparableKeysByBuyerId> indexNode = mBuyerRoot;
        while(!indexNode.isLeafNode()){
            DiskLoc diskLoc = indexNode.search(key);
            if(diskLoc==null)return null;
            indexNode = indexExtentManager.getIndexNodeFromDiskLoc(diskLoc);
        }
        DiskLoc diskLoc = indexNode.search(key);
        if(diskLoc==null)return null;
        return storeExtentManager.getRowFromDiskLoc(diskLoc);
    }

    public Deque<Row> queryOrderDataByBuyerCreateTime(long startTime,long endTime,String buyerid){
        ComparableKeysByBuyerCreateTimeOrderId startKey = new ComparableKeysByBuyerCreateTimeOrderId(
                buyerid,startTime,Long.MIN_VALUE,null
        );
        ComparableKeysByBuyerCreateTimeOrderId endKey = new ComparableKeysByBuyerCreateTimeOrderId(
                buyerid,endTime-1,Long.MAX_VALUE,null
        );
        /**
         * 对磁盘中符合条件的队列进行层序遍历
         */
        return levelTraversal(mBuyerCreateTimeOrderRoot,startKey,endKey);
    }

    public Queue<Row> queryOrderDataByGoodid(String goodid){
        ComparableKeysByGoodOrderId minKey = new ComparableKeysByGoodOrderId(goodid,Long.MIN_VALUE);
        ComparableKeysByGoodOrderId maxKey = new ComparableKeysByGoodOrderId(goodid,Long.MAX_VALUE);
        /**
         * 对磁盘进行层序遍历
         */
        return levelTraversal(mGoodOrderRoot,minKey,maxKey);
    }

    private <V extends Comparable&Serializable&Indexable> LinkedList<Row> levelTraversal(IndexNode root, V minKey, V maxKey){
        LinkedList<Row> result = new LinkedList<>();

        Queue<IndexNode> nodes = new LinkedList<>();
        nodes.add(root);
        while(!nodes.isEmpty()){
            IndexNode node = nodes.remove();
            if(node.isLeafNode()){
                Queue<DiskLoc> diskLocs = node.searchBetween(minKey,maxKey);
                DiskLoc diskLoc = diskLocs.poll();
                while(diskLoc!=null){
                    Row row = storeExtentManager.getRowFromDiskLoc(diskLoc);
                    result.add(row);
                    diskLoc = diskLocs.poll();
                }
            }else {
                Queue<DiskLoc> diskLocs = node.searchBetween(minKey,maxKey);
                if(diskLocs!=null) {
                    while (!diskLocs.isEmpty()) {
                        DiskLoc diskLoc = diskLocs.remove();
                        IndexNode indexNode = indexExtentManager.getIndexNodeFromDiskLoc(diskLoc);
                        nodes.add(indexNode);
                    }
                }
            }
        }
        return result;
    }

}
