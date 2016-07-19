package com.alibaba.middleware.race.storage;

import com.alibaba.middleware.race.RaceConf;

import java.io.Serializable;
import java.util.Vector;

/**
 * Created by xiyuanbupt on 7/13/16.
 */
abstract class IndexNode implements Serializable{
    //all type of node have data, parent and capacity
    protected Vector<IndexDataNode> data;
    //根据DeskLoc 可以反序列化出一个个
    protected DiskLoc parent;
    //每一个node 最大容量
    protected int maxsize = RaceConf.INDEXNODEMAXSIZE;

    public boolean isLeafNode(){
        return this.getClass().getName().trim().equals("IndexLeafNode");
    }

    //both types of node need to insert and search
    abstract IndexNode insert(IndexDataNode indexDataNode);
    abstract DiskLoc search(IndexDataNode indexDataNode);

    protected boolean isFull(){
        return data.size() == maxsize -1;
    }

    public IndexDataNode getIndexDataAt(int index){
        return (IndexDataNode)data.elementAt(index);
    }

    //
    protected void progagate(IndexDataNode indexDataNode,IndexNode right){

    }
}
