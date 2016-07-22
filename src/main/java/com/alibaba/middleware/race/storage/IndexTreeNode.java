package com.alibaba.middleware.race.storage;

import java.io.Serializable;
import java.util.List;
import java.util.Vector;

/**
 * Created by xiyuanbupt on 7/21/16.
 */
public class IndexTreeNode<T extends Serializable & Comparable> extends IndexNode<T> {

    protected Vector<DiskLoc> pointer;

    public IndexTreeNode(){
        super();
        this.pointer = new Vector<>(maxsize);
        this.data = new Vector<>(maxsize);
    }

    @Override
    IndexNode insert(T t) {
        return null;
    }

    @Override
    DiskLoc search(T t) {
        int size = data.size();
        if(t.compareTo(data.firstElement())<0){
            return null;
        }
        for(int i = 0;i<size-1;++i){
            int ret = t.compareTo(data.get(i+1));
            if(ret<0)return pointer.get(i);
        }
        return pointer.lastElement();
    }

    @Override
    List<DiskLoc> searchBetween(Serializable start, Serializable end) {
        return null;
    }

    public synchronized IndexTreeNode addPointer(DiskLoc diskLoc){
        if(isFull()){
            LOG.info("some error happend , pointer element count is greater than max count");
            System.exit(-1);
        }
        pointer.add(diskLoc);
        return this;
    }
}
