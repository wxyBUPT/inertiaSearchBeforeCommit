package com.alibaba.middleware.race.storage;

import com.alibaba.middleware.race.RaceConf;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

/**
 * Created by xiyuanbupt on 7/13/16.
 */
abstract class IndexNode<T extends Serializable> implements Serializable,Iterable<T>{
    //all type of node have data, parent and capacity
    protected Vector<T> data;
    //每一个node 最大容量
    protected int maxsize = RaceConf.INDEXNODEMAXSIZE;

    public boolean isLeafNode(){
        return this.getClass().getName().trim().equals("IndexLeafNode");
    }

    //both types of node need to insert and search
    abstract IndexNode insert(T t);
    abstract DiskLoc search(T t);

    /**
     * 搜索key 的值介于 start 和 end 之间的diskLoc 的值
     * @param start
     * @param end
     * @return
     */
    abstract List<DiskLoc> searchBetween(T start,T end);
    public synchronized IndexNode appendData(T t) throws RuntimeException{
        if(isFull()){
            throw new RuntimeException("node is full ,there is some bug may be");
        }
        data.add(t);

        return this;
    }
    /**
     * Judge if node is full
     * @return
     */
    public boolean isFull(){
        return data.size() == maxsize ;
    }

    public int size(){
        return data.size();
    }

    public T getDataAt(int index){
        return (T)data.elementAt(index);
    }

    @Override
    public Iterator<T> iterator(){
        return data.iterator();
    }
}
