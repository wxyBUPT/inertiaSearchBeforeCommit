package com.alibaba.middleware.race.storage;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;

/**
 * Created by xiyuanbupt on 7/19/16.
 */
public class IndexLeafNode<T extends Serializable & Comparable & Indexable> extends IndexNode<T>{

    @Override
    Queue<DiskLoc> searchBetween(T min, T max) {
        Queue<DiskLoc> diskLocs = new LinkedList<>();


        int size = data.size();
        if(max.compareTo(data.firstElement())<0){
            return null;
        }
        for(int i = 0;i<size;++i){
            T t = data.get(i);
            if(max.compareTo(t)<0)break;
            if(min.compareTo(t)<=0&&max.compareTo(t)>=0){
                diskLocs.add(t.getDataDiskLoc());
            }
        }
        return diskLocs;
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
        int hi = data.size() -1;
        int mid;
        while (lo<=hi){
            mid = lo +(hi-lo)/2;
            int ret = t.compareTo(data.get(mid));
            if(ret<0)hi = mid-1;
            else if(ret>0)lo = mid+1;
            else return data.get(mid).getDataDiskLoc();
        }
        return null;

    }
}
