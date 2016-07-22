package com.alibaba.middleware.race.storage;

/**
 * Created by xiyuanbupt on 7/19/16.
 */

import com.alibaba.middleware.race.codec.SerializationUtils;
import com.alibaba.middleware.race.models.Row;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
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
     * 每个文件的大小都是 1G
     */
    private static long singleFileSize = 1073741824;
    private int nStoreFiles;
    private int nIndexFiles;
    private String nameSpace;
    private ArrayList<String> storeFloders;

    /**
     * 用于记录文件标号和对应文件的关系记录
     */
    private ConcurrentHashMap<Integer,MappedByteBuffer> storeMap = new ConcurrentHashMap<>();


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
        MappedByteBuffer out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE,0,singleFileSize);

        FileInfoBean fib = new FileInfoBean(out,nIndexFiles);

        nIndexFiles ++;
        return fib;
    }

    /**
     * 从磁盘位置获得Row
     * @param diskLoc
     * @return
     */
    public synchronized Row getRowFromDiskLoc(DiskLoc diskLoc){
        int _a = diskLoc.get_a();
        int ofs = diskLoc.getOfs();
        int size = diskLoc.getSize();
        byte[] byteRow = new byte[size];
        MappedByteBuffer buffer = this.storeMap.get(_a);
        int position = buffer.position();
        buffer.position(ofs);
        buffer.get(byteRow);
        buffer.position(position);
        return (Row) SerializationUtils.deserialize(byteRow);
    }
}