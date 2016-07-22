package com.alibaba.middleware.race.storage;

import com.alibaba.middleware.race.models.Row;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByBuyerId;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByGoodId;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByOrderId;

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

    private IndexExtentManager indexExtentManager;
    private FileManager fileManager;

    public IndexNameSpace(){
        indexExtentManager = IndexExtentManager.getInstance();
        fileManager = FileManager.getInstance();
    }

    public Row queryOrderDataByOrderId(Long orderId){
        ComparableKeysByOrderId key = new ComparableKeysByOrderId(orderId,null);
        IndexNode<ComparableKeysByOrderId> indexNode = indexExtentManager.getIndexNodeFromDiskLoc(orderRoot);
        while(!indexNode.isLeafNode()){
            indexNode = indexExtentManager.getIndexNodeFromDiskLoc(indexNode.search(key));
        }
        return fileManager.getRowFromDiskLoc(indexNode.search(key));
    }

    public Row queryGoodDataByGoodId(String goodId){
        ComparableKeysByGoodId key = new ComparableKeysByGoodId(goodId,null);
        IndexNode<ComparableKeysByGoodId> indexNode = indexExtentManager.getIndexNodeFromDiskLoc(goodRoot);
        while (!indexNode.isLeafNode()){
            indexNode = indexExtentManager.getIndexNodeFromDiskLoc(indexNode.search(key));
        }
        return fileManager.getRowFromDiskLoc(indexNode.search(key));
    }

    public Row queryBuyerDataByBuyerId(String buyerId){
        ComparableKeysByBuyerId key = new ComparableKeysByBuyerId(buyerId,null);
        IndexNode<ComparableKeysByBuyerId> indexNode = indexExtentManager.getIndexNodeFromDiskLoc(buyerRoot);
        while(!indexNode.isLeafNode()){
            indexNode = indexExtentManager.getIndexNodeFromDiskLoc(indexNode.search(key));
        }
        return fileManager.getRowFromDiskLoc(indexNode.search(key));
    }


}
