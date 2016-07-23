package com.alibaba.middleware.race.storage;

/**
 * Created by xiyuanbupt on 7/19/16.
 */

import com.alibaba.middleware.race.models.Row;

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
     * 存储原始数据的文件,第几个文件用于创建文件名称
     */
    private int nStoreFiles;

    /**
     * index 和原始文件处于不同的命名空间
     */
    private int nIndexFiles;

    /**
     * nameSpace 用于给文件起名
     */
    private final String nameSpace;

    /**
     * 题目中给出的外存空间
     */
    private ArrayList<String> storeFloders;


    /**
     * 一下两条都是暂时作为记录,今后不一定被使用到
     * 用于记录文件标号和对应文件的关系记录,因为文件管理是单例,故直接初始化,不一定被使用
     */
    private HashMap<Integer,StoreFile> storeMap = new HashMap<>();

    /**
     * 用于记录索引文件标号和对应文件的关系记录
     * @return
     */
    private HashMap<Integer,StoreFile> indexMap = new HashMap<>();

    public synchronized StoreFile createNewStoreFile() {
        String dirBase = storeFloders.get(nStoreFiles%3);
        String dir = dirBase + "/" + nameSpace + nStoreFiles;
        LOG.info("Create store file, file name is : " + dir);
        StoreFile storeFile = new StoreFile(dir,nStoreFiles);
        storeMap.put(nStoreFiles,storeFile);
        nStoreFiles ++;
        return storeFile;
    }

    public synchronized StoreFile createNewIndexFile() {
        String dirBase = storeFloders.get(nIndexFiles%3);
        String dir = dirBase + "/" + nameSpace + ".index." + nIndexFiles;
        LOG.info("Create index file , file name is : " + dir);
        StoreFile storeFile = new StoreFile(dir,nIndexFiles);
        indexMap.put(nIndexFiles,storeFile);
        nIndexFiles ++;
        return storeFile;
    }
}
