package com.alibaba.middleware.race.storage;

import java.io.Serializable;
import java.util.List;
import java.util.Vector;

/**
 * Created by xiyuanbupt on 7/19/16.
 */
public class IndexLeafNode<T extends Serializable> extends IndexNode<T>{

    @Override
    List<DiskLoc> searchBetween(T start, T end) {
        return null;
    }

    public IndexLeafNode(){
        this.data = new Vector<>(maxsize);
    }

    @Override
    IndexNode insert(T t) {
        return null;
    }

    @Override
    DiskLoc search(T t) {
        return null;
    }
}
