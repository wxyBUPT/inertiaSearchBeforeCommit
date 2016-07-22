package com.alibaba.middleware.race.storage;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Vector;

/**
 * Created by xiyuanbupt on 7/19/16.
 */
public class IndexLeafNode<T extends Serializable & Comparable & Indexable> extends IndexNode<T>{

    @Override
    List<DiskLoc> searchBetween(T start, T end) {
        return null;
    }

    public IndexLeafNode(){
        super();
        this.data = new Vector<>(maxsize);
    }

    @Override
    IndexNode insert(T t) {
        return null;
    }

    /**
     * A simple binary search
     * @param t
     * @return
     */
    @Override
    DiskLoc search(T t) {
        int lo = 0;
        int hi = data.size();
        while (lo<=hi){
            int mid = lo +(hi-lo)/2;
            int ret = t.compareTo(data.get(mid));
            if(ret<0)hi = mid-1;
            else if(ret>0)lo = mid+1;
            else return data.get(mid).getDataDiskLoc();
        }
        return null;

    }
}
