package com.alibaba.middleware.race.storage;

import com.alibaba.middleware.race.RaceConf;
import com.alibaba.middleware.race.codec.SerializationUtils;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
    private ConcurrentHashMap<Integer,MappedByteBuffer> indexMap = new ConcurrentHashMap<>();

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
     * 请求分配磁盘空间为size byte 的空间,申请的磁盘空间可能大于需要的size
     * @param size
     * @return
     */
    public synchronized DiskLoc mallocDiskLoc(int size)throws IOException{
        /**
         * 如果有磁盘空间满足要求
         */
        //假设硬盘空间足够
        //Iterator<DiskLoc> it = freeDiskLocs.iterator();
        //DiskLoc diskLoc;
        //while(it.hasNext()){
        //    diskLoc = it.next();
        //    if(diskLoc.getSize()>=size){
        //        return diskLoc;
        //    }
        //}
        /**
         * If current file can't save more
         */
        if(currentFile.remaining()<size+4){
            createNewIndexFile();
        }
        /**
         * move buffer position for size
         * and
         * make new disk lock
         */
        DiskLoc diskLoc = new DiskLoc(this.currentFileNum,this.currentOff,StoreType.INDEXHEADER,size);
        currentOff += size;
        currentFile.position(currentOff);
        return diskLoc;
    }

    public synchronized byte[] getBytesFromDiskLoc(DiskLoc diskLoc){
        int _a = diskLoc.get_a();
        int ofs = diskLoc.getOfs();
        int size = diskLoc.getSize();
        byte[] bytes = new byte[size];
        MappedByteBuffer buffer = this.indexMap.get(_a);
        int position = buffer.position();
        buffer.position(ofs);
        buffer.get(bytes);
        buffer.position(position);
        return bytes;
    }

    public synchronized void putBytesToDiskLoc(DiskLoc diskLoc,byte[] bytes){
        int _a = diskLoc.get_a();
        int ofs = diskLoc.getOfs();
        MappedByteBuffer buffer = this.indexMap.get(_a);
        int position = buffer.position();
        buffer.position(ofs);
        buffer.put(bytes);
        buffer.position(position);
    }

    private synchronized void createNewIndexFile() throws IOException{
        FileInfoBean fib = fileManager.createIndexFile();
        currentFile = fib.getBuffer();
        currentFileNum = fib.getfileN();
        indexMap.put(currentFileNum,currentFile);
        currentOff = 0;
    }

    public static void main(String[] args){
        List<String> storeFolders = new ArrayList<>();
        storeFolders.add("./dir0");
        storeFolders.add("./dir1");
        storeFolders.add("./dir2");
        IndexExtentManager indexExtentManager = IndexExtentManager.getInstance(storeFolders, "tianchi");
        DiskLoc diskLoc;
        LOG.info("finsh");
    }

}
