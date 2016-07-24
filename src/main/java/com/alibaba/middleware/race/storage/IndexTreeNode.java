package com.alibaba.middleware.race.storage;

import java.io.Serializable;
import java.util.*;

/**
 * Created by xiyuanbupt on 7/21/16.
 */
public class IndexTreeNode<T extends Serializable & Comparable & Indexable> extends IndexNode<T> {

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
        //使用二分查找
        int lo = 0;
        int hi = data.size() -1;
        int mid;
        while (lo<=hi){
            mid = lo + (hi-lo)/2;
            if(mid==(size -1)){
                break;
            }
            int ret1 = t.compareTo(data.get(mid));
            int ret2 = t.compareTo(data.get(mid+1));
            if(ret1<0)hi = mid-1;
            else if(ret2>=0)lo=mid+1;
            else return pointer.get(mid);
        }
        //for(int i = 0;i<size-1;++i){
        //    int ret = t.compareTo(data.get(i+1));
        //    if(ret<0)return pointer.get(i);
        //}
        //return pointer.lastElement();
        return pointer.lastElement();
    }

    @Override
    Queue<DiskLoc> searchBetween(T min, T max) {
        /**
         * if value <= end && nextValue > min , add this element pointer
         * return all pointer
         */
        Queue<DiskLoc> diskLocs = new LinkedList<>();
        int size = data.size();
        if(max.compareTo(data.firstElement())<0){
            return null;
        }
        for(int i = 0;i<size-1;++i){
            int ret = max.compareTo(data.get(i));
            if(ret<0)return diskLocs;
            ret = min.compareTo(data.get(i+1));
            if(ret<0)diskLocs.add(pointer.get(i));
        }
        /**
         * Handle the last element
         */
        if(max.compareTo(data.lastElement())>=0){
            diskLocs.add(pointer.lastElement());
        }
        return diskLocs;
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
