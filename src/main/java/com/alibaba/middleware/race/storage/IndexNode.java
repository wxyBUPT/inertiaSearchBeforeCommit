package com.alibaba.middleware.race.storage;

import com.alibaba.middleware.race.RaceConf;

import java.io.Serializable;
import java.util.Vector;

/**
 * Created by xiyuanbupt on 7/13/16.
 */
abstract class IndexNode<T extends Serializable> implements Serializable{
    //all type of node have data, parent and capacity
    protected Vector<T> data;
    //根据DeskLoc 可以反序列化出IndexTreeNode
    protected DiskLoc parent;
    //每一个node 最大容量
    protected int maxsize = RaceConf.INDEXNODEMAXSIZE;

    public boolean isLeafNode(){
        return this.getClass().getName().trim().equals("IndexLeafNode");
    }

    //both types of node need to insert and search
    abstract IndexNode insert(T t);
    abstract DiskLoc search(T t);

    /**
     * Judge if node is full
     * full condition is data.size() == maxsize -1
     * @return
     */
    protected boolean isFull(){
        return data.size() == maxsize -1;
    }

    public int size(){
        return data.size();
    }

    public T getDataAt(int index){
        return (T)data.elementAt(index);
    }

    /**
     *
     * @param dnode
     * @param right
     */
    protected void propagate(T dnode,IndexNode right){

    }
}
