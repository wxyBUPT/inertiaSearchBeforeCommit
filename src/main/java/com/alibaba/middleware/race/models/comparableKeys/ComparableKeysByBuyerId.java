package com.alibaba.middleware.race.models.comparableKeys;

import com.alibaba.middleware.race.storage.DiskLoc;

import java.io.Serializable;

/**
 * Created by xiyuanbupt on 7/14/16.
 */
public class ComparableKeysByBuyerId implements Comparable<ComparableKeysByBuyerId>,Serializable{

    @Override
    public int compareTo(ComparableKeysByBuyerId o) {
        if(this.buyerId==null||o.buyerId==null){
            throw new RuntimeException("Bad keys");
        }
        return this.buyerId.compareTo(o.buyerId);
    }

    private DiskLoc dataDiskLoc;
    private String buyerId;

    public ComparableKeysByBuyerId(String buyerId,DiskLoc dataDiskLoc){
        this.buyerId = buyerId;
        this.dataDiskLoc = dataDiskLoc;
    }

    public DiskLoc getDataDiskLoc(){
        return dataDiskLoc;
    }

    @Override
    public String toString(){
        return "ComparableKeysByBuyerId: buyerId : " + buyerId;
    }
}
