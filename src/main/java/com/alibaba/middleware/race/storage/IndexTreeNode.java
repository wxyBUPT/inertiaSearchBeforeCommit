package com.alibaba.middleware.race.storage;

import java.io.Serializable;
import java.util.List;
import java.util.Vector;

/**
 * Created by xiyuanbupt on 7/21/16.
 */
public class IndexTreeNode<T extends Serializable> extends IndexNode{

    protected Vector<DiskLoc> pointer;

    public IndexTreeNode(){
        super();
        this.pointer = new Vector<>(maxsize);
        this.data = new Vector(maxsize);
    }

    @Override
    IndexNode insert(Serializable serializable) {
        return null;
    }

    @Override
    DiskLoc search(Serializable serializable) {
        return null;
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
