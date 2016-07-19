package com.alibaba.middleware.race.storage;

import java.io.Serializable;
import java.nio.MappedByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by xiyuanbupt on 7/11/16.
 * 数据库的命名空间,负责管理数据库存储的所有数据文件,索引文件
 * 负责磁盘的负载均衡
 * 在namespace 中完成数据的存储于排序工作
 */
public class Namespace {

    private static final Logger LOG = Logger.getLogger(Namespace.class.getName());
    //使用os 的mmnp 管理所有的文件
    //命名空间所有的文件
    private List<MappedByteBuffer> storeFiles;

    private int fileCount ;
    private String ns;

    private List<String> storeFolders;
    //索引的个数
    private int nIndex;

    //创建名称为 ns 的namespace
    private Namespace(String ns,List<String> storeFolders){
        this.ns = ns;
        this.storeFolders = storeFolders;
    }

    //一个ns 只能对应一个namespace
    private static Map<String,Namespace> namespaces = new HashMap<>();
    public static synchronized Namespace mkInstance(String ns,List<String> storeFolders,List<String> originFiles){
        Namespace namespace = namespaces.get(ns);
        if(namespace != null){
            return namespace;
        }
        else {
            return new Namespace(ns,storeFolders);
        }
    }

}

//根据如下数据格式在btree中创建数据索引
class IndexInfo <Key extends Comparable<Key>> implements Serializable{

    Key key;
    DiskLoc diskLoc;

    IndexInfo(Key key,DiskLoc diskLoc){
        this.key = key;
        this.diskLoc = diskLoc;
    }
}

