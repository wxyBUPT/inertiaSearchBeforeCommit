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
        Integer start = binaryFindStart(min);
        System.out.println("Start is " + start);
        if(start==null)return diskLocs;
        Integer end = binaryFindEnd(max);
        if(end==null)return diskLocs;
        for(int i = start;i<=end;i++){
            diskLocs.add(data.get(i).getDataDiskLoc());
        }

        //int size = data.size();
        //if(max.compareTo(data.firstElement())<0){
        //    return null;
        //}
        //for(int i = 0;i<size;++i){
        //    T t = data.get(i);
        //    if(min.compareTo(t)<=0&&max.compareTo(t)>=0){
        //        diskLocs.add(t.getDataDiskLoc());
        //    }
        //}
        return diskLocs;
    }

    public IndexLeafNode(){
        super();
        this.data = new Vector<>(maxsize);
    }

    private Integer binaryFindStart(T value){
        int lo = 0;
        int hi = data.size() -1;
        int mid;
        while(lo<=hi){
            if(lo==hi)return lo;
            mid = lo + (hi-lo)/2;
            System.out.println("min is : " + lo + "mid is " + mid
                    + "high is " + hi);
            int ret1 = value.compareTo(data.get(mid));
            if(mid==hi){
                if(ret1==0)return mid;
                else return null;
            }
            int ret2 = value.compareTo(data.get(mid+1));
            if(ret1<0)hi = mid -1;
            else if(ret2>=0)lo = mid+1;
            else if(ret1==0)return mid;
            else return mid+1;
        }
        return null;
    }

    private Integer binaryFindEnd(T value){
        int lo = 0;
        int hi = data.size()-1;
        while(lo<=hi){
            if(lo==hi)return lo;
            int mid = lo+(hi-lo)/2;
            int ret1 = value.compareTo(data.get(mid));
            if(mid== hi){
                if(ret1>=0)return mid;
                else return null;
            }
            int ret2 = value.compareTo(data.get(mid+1));
            if(ret1<0)hi= mid-1;
            else if(ret2>=0)lo = mid +1;
            else return mid;
        }
        return null;
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
