package com.alibaba.middleware.race.models.comparableKeys;

import com.alibaba.middleware.race.storage.DiskLoc;

import java.io.Serializable;

/**
 * Created by xiyuanbupt on 7/14/16.
 */
public class ComparableKeysByOrderId implements Comparable<ComparableKeysByOrderId> ,Serializable{

    private DiskLoc dataDiskLoc;
    private Long orderId;

    public ComparableKeysByOrderId(String orderId,DiskLoc diskLoc){
        this.dataDiskLoc = diskLoc;
        this.orderId = Long.parseLong(orderId);
    }

    public ComparableKeysByOrderId(Long orderId,DiskLoc diskLoc){
        this.dataDiskLoc = diskLoc;
        this.orderId = orderId;
    }

    public DiskLoc getDataDiskLoc(){
        return this.dataDiskLoc;
    }

    @Override
    public int compareTo(ComparableKeysByOrderId o) {
        return this.orderId.compareTo(o.orderId);
    }
}
