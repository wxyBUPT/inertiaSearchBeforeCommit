package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.cache.AvlTree;
import com.alibaba.middleware.race.storage.DiskLoc;
import com.alibaba.middleware.race.storage.StoreType;

import java.io.Serializable;
import java.util.LinkedList;

/**
 * Created by xiyuanbupt on 7/19/16.
 * 提供工具类,将本地缓存的索引同步到磁盘
 */
public class FlushUtil<T extends Comparable<? super T>> {
    public LinkedList<DiskLoc> flushAvlToDisk(AvlTree<T> from , LinkedList<DiskLoc> diskLocs){
        for(T t :from){

        }
        return diskLocs;
    }

    public DiskLoc buildBPlusTree(LinkedList<DiskLoc> diskLocs){
        System.out.println("This method hasn't been finsh ");
        return new DiskLoc(0,0,StoreType.INDEXHEADER,1);
    }
}
