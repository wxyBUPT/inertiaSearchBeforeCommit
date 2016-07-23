package com.alibaba.middleware.race.storage;


import com.alibaba.middleware.race.codec.SerializationUtils;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByOrderId;

import java.util.*;

/**
 * Created by xiyuanbupt on 7/19/16.
 * 负责管理所有IndexExtent
 */
public class IndexExtentManager extends ExtentManager{

    protected static IndexExtentManager indexExtentManager;

    /**
     * Make it a singloton
     */
    private IndexExtentManager(){
        super();
    }

    public static synchronized IndexExtentManager getInstance(){
        if(indexExtentManager==null){
            indexExtentManager = new IndexExtentManager();
        }
        return indexExtentManager;
    }

    @Override
    protected StoreFile newFile() {
        return fileManager.createNewIndexFile();
    }

    /**
     * 不一定被使用到的方法,更多被使用到的是 getIndexNodeFromDiskLoc
     * @param diskLoc 磁盘中的位置
     * @return
     */
    public IndexLeafNode getIndexLeafNodeFromDiskLoc(DiskLoc diskLoc){
        byte[] bytes = getBytes(diskLoc);
        return (IndexLeafNode)SerializationUtils.deserialize(bytes);
    }

    public IndexLeafNode getIndexLeafNodeFromDiskLocForInsert(DiskLoc diskLoc){
        byte[] bytes = getBytesForInsert(diskLoc);
        return (IndexLeafNode)SerializationUtils.deserialize(bytes);
    }

    /**
     * 经常被使用到的方法,从磁盘位置获得indexNode
     * @param diskLoc 磁盘位置
     * @return
     */
    public IndexNode getIndexNodeFromDiskLoc(DiskLoc diskLoc){
        return (IndexNode)SerializationUtils.deserialize(getBytes(diskLoc));
    }

    /**
     * 查询使用的方法,查询后恢复 extent 的ofs
     * @param diskLoc 磁盘位置
     * @return
     */
    public IndexNode getIndexNodeFromDiskLocForInsert(DiskLoc diskLoc){
        return (IndexNode)SerializationUtils.deserialize(getBytesForInsert(diskLoc));
    }

    /**
     * 向Extent 中插入索引,此方法并不常用,或者可能不被使用
     * @param indexLeafNode 要插入的叶子节点
     * @return
     */
    public DiskLoc putIndexLeafNode(IndexLeafNode indexLeafNode) {
        byte[] bytes = SerializationUtils.serialize(indexLeafNode);
        DiskLoc diskLoc = putBytes(bytes);
        diskLoc.storeType = StoreType.INDEXLEAFNODE;
        return diskLoc;
    }

    /**
     * 向Extent 中插入索引(叶子节点,非叶子节点),此方法经常被使用
     * @param indexNode 索引节点
     * @return
     */
    public DiskLoc putIndexNode(IndexNode indexNode){
        byte[] bytes = SerializationUtils.serialize(indexNode);
        DiskLoc diskLoc = putBytes(bytes);
        diskLoc.storeType = StoreType.INDEXNODE;
        return diskLoc;
    }

    public static void main(String[] args){
        List<String> storeFolders = new ArrayList<>();
        storeFolders.add("./dir0");
        storeFolders.add("./dir1");
        storeFolders.add("./dir2");
        IndexExtentManager indexExtentManager = IndexExtentManager.getInstance();
        LOG.info("finsh");
        IndexLeafNode<ComparableKeysByOrderId> indexLeafNode = new IndexLeafNode<>();
        indexLeafNode.appendData(new ComparableKeysByOrderId(123L,new DiskLoc(0,0,StoreType.INDEXHEADER,1)));
        DiskLoc diskLoc = indexExtentManager.putIndexLeafNode(indexLeafNode);
        IndexLeafNode<ComparableKeysByOrderId> indexLeafNode1 = indexExtentManager.getIndexLeafNodeFromDiskLoc(diskLoc);
        System.out.println(indexLeafNode1);
        for(ComparableKeysByOrderId comparableKeysByOrderId:indexLeafNode1){
            System.out.println(comparableKeysByOrderId);
        }
        System.out.println(indexLeafNode1.isLeafNode());
        IndexNode<ComparableKeysByOrderId> idIndexNode = indexExtentManager.getIndexNodeFromDiskLoc(diskLoc);
        System.out.println(idIndexNode.isLeafNode());
    }

}
