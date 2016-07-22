package com.alibaba.middleware.race.storage;


import com.alibaba.middleware.race.codec.SerializationUtils;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByOrderId;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * Created by xiyuanbupt on 7/19/16.
 */
public class IndexExtentManager {

    private static Logger LOG = Logger.getLogger(IndexExtentManager.class.getName());
    protected static IndexExtentManager indexExtentManager;
    protected FileManager fileManager;
    /**
     * 当前的文件与在文件内的偏移量
     */
    protected MappedByteBuffer currentFile;
    protected int currentOff;
    protected int currentFileNum;

    /**
     * 用于记录索引标号与对应文件的关系记录
     */
    private HashMap<Integer,MappedByteBuffer> indexMap = new HashMap<>();
    private HashMap<Integer,Lock> indexLockMap = new HashMap<>();

    private IndexExtentManager(Collection<String> storeFloders,String nameSpace){
        LOG.info("init singlton");
        this.fileManager = FileManager.getInstance(storeFloders,nameSpace);
        try {
            createNewIndexFile();
        }catch (Exception e){
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static synchronized IndexExtentManager getInstance(Collection<String> storeFloders, String nameSpace){
        if(indexExtentManager==null){
            indexExtentManager = new IndexExtentManager(storeFloders,nameSpace);
        }
        return indexExtentManager;
    }

    /**
     * after init,
     * @return
     */
    public static synchronized IndexExtentManager getInstance(){
        if(indexExtentManager==null){
            System.out.println("Get indexExtentManager fail, indexExtentManager hasn't been init");
            System.exit(-1);
        }
        return indexExtentManager;
    }

    public synchronized IndexLeafNode getIndexLeafNodeFromDiskLoc(DiskLoc diskLoc){
        int _a = diskLoc.get_a();
        int ofs = diskLoc.getOfs();
        int size = diskLoc.getSize();
        byte[] bytes = new byte[size];
        MappedByteBuffer buffer = this.indexMap.get(_a);
        int position = buffer.position();
        buffer.position(ofs);
        buffer.get(bytes);
        buffer.position(position);
        return (IndexLeafNode) SerializationUtils.deserialize(bytes);
    }

    public synchronized IndexNode getIndexNodeFromDiskLoc(DiskLoc diskLoc){
        int _a = diskLoc.get_a();
        int ofs = diskLoc.getOfs();
        int size = diskLoc.getSize();
        byte[] bytes = new byte[size];
        MappedByteBuffer buffer = this.indexMap.get(_a);
        int position = buffer.position();
        buffer.position(ofs);
        buffer.get(bytes);
        buffer.position(position);
        return (IndexNode) SerializationUtils.deserialize(bytes);
    }

    /**
     * 查询使用的方法,不恢复文件位置,创建索引阶段不可使用
     * @param diskLoc
     * @return
     */
    public IndexNode getIndexNodeFromDiskLocForSearch(DiskLoc diskLoc){
        int _a = diskLoc.get_a();
        int ofs = diskLoc.getOfs();
        int size = diskLoc.getSize();
        byte[] bytes = new byte[size];
        MappedByteBuffer buffer = indexMap.get(_a);
        Lock lock = indexLockMap.get(_a);
        lock.lock();
        buffer.position(ofs);
        buffer.get(bytes);
        lock.unlock();
        return (IndexNode)SerializationUtils.deserialize(bytes);
    }



    public synchronized DiskLoc putIndexLeafNode(IndexLeafNode indexLeafNode) {
        byte[] bytes = SerializationUtils.serialize(indexLeafNode);
        int size = bytes.length;
        /**
         * If file can't save more, create new File
         */
        try {
            updataFile(size);
        } catch (IOException e){
            e.printStackTrace();
            System.exit(-1);
        }
        currentFile.put(bytes);
        DiskLoc diskLoc = new DiskLoc(currentFileNum,this.currentOff,StoreType.INDEXLEAFNODE,size);
        currentOff += size;
        return diskLoc;
    }

    public synchronized DiskLoc putIndexNode(IndexNode indexNode){
        byte[] bytes = SerializationUtils.serialize(indexNode);
        int size = bytes.length;
        /**
         * If file can't save more, create new File
         */
        try{
            updataFile(size);
        }catch (IOException e){
            e.printStackTrace();
            System.exit(-1);
        }
        currentFile.put(bytes);
        DiskLoc diskLoc = new DiskLoc(currentFileNum,this.currentOff,StoreType.INDEXNODE,size);
        currentOff += size;
        return diskLoc;

    }

    private synchronized void updataFile(int size) throws IOException{
        if(currentFile.remaining()<size+4){
            createNewIndexFile();
        }
    }


    private synchronized void createNewIndexFile() throws IOException{
        FileInfoBean fib = fileManager.createIndexFile();
        currentFile = fib.getBuffer();
        currentFileNum = fib.getfileN();
        indexMap.put(currentFileNum,currentFile);
        indexLockMap.put(currentFileNum,new ReentrantLock());
        currentOff = 0;
    }

    public static void main(String[] args){
        List<String> storeFolders = new ArrayList<>();
        storeFolders.add("./dir0");
        storeFolders.add("./dir1");
        storeFolders.add("./dir2");
        IndexExtentManager indexExtentManager = IndexExtentManager.getInstance(storeFolders, "tianchi");
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
