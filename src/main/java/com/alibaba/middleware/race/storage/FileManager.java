package com.alibaba.middleware.race.storage;

/**
 * Created by xiyuanbupt on 7/19/16.
 */

import com.alibaba.middleware.race.models.Row;
import com.alibaba.middleware.race.models.RowKV;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.logging.Logger;

/**
 * 负责管理所有文件的类
 */
public class FileManager {

    private static Logger LOG = Logger.getLogger(FileManager.class.getName());

    private static FileManager fileManager;

    private FileManager(Collection<String> storeFloders, String nameSpace){
        if(storeFloders.size()!=3){
            throw new RuntimeException("storeFloders must equal to 3");
        }
        this.storeFloders = (ArrayList<String>)storeFloders;
        nIndexFiles = 0;
        nStoreFiles = 0;
        this.nameSpace = nameSpace;
    }

    public static synchronized FileManager getInstance(Collection<String> storeFloders,String nameSpace){
        if(fileManager == null){
            fileManager = new FileManager(storeFloders,nameSpace);
        }
        return fileManager;
    }

    public static synchronized FileManager getInstance(){
        if(fileManager==null){
            LOG.info("Some bug exist! this shoudn't call first");
            System.exit(-1);
        }
        return fileManager;
    }

    /**
     * 存储原始数据的文件以及Extent 分开,第几个文件用于创建文件名称,第几个extent 用于记录唯一的extent 数目
     */
    private int nStoreFiles;
    private int nStoreExtents;
    private int nIndexFiles;
    private int nIndexExtents;

    /**
     * nameSpace 用于给文件起名
     */
    private String nameSpace;

    /**
     * 能够使用的磁盘空间
     */
    private ArrayList<String> storeFloders;

    /**
     * 用于记录文件标号和对应文件的关系记录,因为文件管理是单例,故直接初始化
     */
    private HashMap<Integer,MappedByteBuffer> storeMap = new HashMap<>();
    /**
     * 用于记录Extent 标号,用于快速定位Extent 位置
     */
    private HashMap<Integer,Extent> extentMap = new HashMap<>();
    private Long singleFileSize;

    public synchronized FileInfoBean createStoreFile() throws IOException {
        String dirBase = storeFloders.get(nStoreFiles%3);
        String dir = dirBase + "/" + nameSpace + nStoreFiles;

        RandomAccessFile memoryMappedFile = new RandomAccessFile(dir,"rw");

        //Mapping a file into memory
        MappedByteBuffer out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE,0,singleFileSize);

        storeMap.put(nStoreFiles,out);
        FileInfoBean fib = new FileInfoBean(out,nStoreFiles);

        nStoreFiles ++;
        return fib;
    }

    public synchronized FileInfoBean createIndexFile() throws IOException{
        String dirBase = storeFloders.get(nIndexFiles%3);
        String dir = dirBase + "/" + nameSpace + ".index." + nIndexFiles;
        LOG.info("Create index file , file name is : " + dir);

        RandomAccessFile memoryMappedFile = new RandomAccessFile(dir,"rw");

        //Mapping a file into memory
        MappedByteBuffer out = memoryMappedFile.getChannel().
                map(FileChannel.MapMode.READ_WRITE,0,singleFileSize);

        FileInfoBean fib = new FileInfoBean(out,nIndexFiles);

        nIndexFiles ++;
        return fib;
    }

    /**
     * 从磁盘位置获得Row,可能最浪费时间的代码就在这段
     * @param diskLoc
     * @return
     */
    public Row getRowFromDiskLoc(DiskLoc diskLoc){
        int _a = diskLoc.get_a();
        int ofs = diskLoc.getOfs();
        int size = diskLoc.getSize();
        byte[] bytes = new byte[size];
        MappedByteBuffer buffer = this.storeMap.get(_a);
        //Lock lock = storeLockMap.get(_a);
        //lock.lock();
        synchronized (this) {
            buffer.position(ofs);
            buffer.get(bytes);
        }
        //lock.unlock();
        String line = new String(bytes);
        return createKVMapFromLine(line);
    }

    private Row createKVMapFromLine(String line) {
        String[] kvs = line.split("\t");
        Row kvMap = new Row();
        for (String rawkv : kvs) {
            int p = rawkv.indexOf(':');
            String key = rawkv.substring(0, p);
            String value = rawkv.substring(p + 1);
            if (key.length() == 0 || value.length() == 0) {
                throw new RuntimeException("Bad data:" + line);
            }
            RowKV kv = new RowKV(key, value);
            kvMap.put(kv.key(), kv);
        }
        return kvMap;
    }
}
